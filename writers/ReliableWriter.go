package writers

import (
	"awesomeProject/utils"
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
)

type ReliableWriterConfig struct {
	MaxCacheSize uint32
	MinChunkSize uint32
	MaxChunkSize uint32
}

type ReliableWriterImpl struct {
	data                    ScatterGatherBuffer
	writtenBytes            uint64
	offset                  uint64
	mutex                   sync.Mutex
	MaxCacheSize            uint32
	MinChunkSize            uint32
	MaxChunkSize            uint32
	isComplete              bool
	isAborted               bool
	suspendChan             chan struct{}
	writeEventsChan         chan struct{}
	unreliableWriterBuilder func() (UnreliableWriter, error)
	unreliableWriter        UnreliableWriter
	resultChan              chan error
}

func NewReliableWriterImplWithBuilder(ctx context.Context, writerBuilder func() (UnreliableWriter, error), config ReliableWriterConfig) (*ReliableWriterImpl, error) {
	writer, err := writerBuilder()
	if err != nil {
		return nil, err
	}
	if writer == nil {
		panic("UnreliableWriterBuilder returned nil")
	}
	rw := &ReliableWriterImpl{
		data:                    NewScatterGatherBuffer(),
		isComplete:              false,
		suspendChan:             make(chan struct{}, 1),
		writeEventsChan:         make(chan struct{}, 1),
		resultChan:              make(chan error, 1),
		unreliableWriterBuilder: writerBuilder,
		unreliableWriter:        writer,
		MaxCacheSize:            config.MaxCacheSize,
		MinChunkSize:            config.MinChunkSize,
		MaxChunkSize:            config.MaxChunkSize,
	}
	rw.launchWriting(ctx)
	return rw, nil
}

func NewReliableWriterImpl(ctx context.Context, writer UnreliableWriter, config ReliableWriterConfig) *ReliableWriterImpl {
	rw := &ReliableWriterImpl{
		data:             NewScatterGatherBuffer(),
		isComplete:       false,
		suspendChan:      make(chan struct{}, 1),
		writeEventsChan:  make(chan struct{}, 1),
		resultChan:       make(chan error, 1),
		unreliableWriter: writer,
		MaxCacheSize:     config.MaxCacheSize,
		MinChunkSize:     config.MinChunkSize,
		MaxChunkSize:     config.MaxChunkSize,
	}
	rw.launchWriting(ctx)
	return rw
}

func (rw *ReliableWriterImpl) WakeUp() {
	select {
	case rw.suspendChan <- struct{}{}:
	default:
	}
}

func (rw *ReliableWriterImpl) SuspendAndWaitForAwake(ctx context.Context) error {
	select {
	case <-rw.suspendChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rw *ReliableWriterImpl) notifyWriteEvent() {
	select {
	case rw.writeEventsChan <- struct{}{}:
	default:
	}
}

func (rw *ReliableWriterImpl) waitForWriteEvent(ctx context.Context) error {
	select {
	case <-rw.writeEventsChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rw *ReliableWriterImpl) WriteAt(ctx context.Context, buf []byte, off int64) error {
	if rw.isComplete {
		return errors.New("write operation is already completed")
	}
	if rw.writtenBytes != uint64(off) {
		return errors.New("buffer size mismatch")
	}

	rw.mutex.Lock()
	rw.data.AddBytes(buf)
	rw.writtenBytes += uint64(len(buf))
	rw.mutex.Unlock()

	rw.notifyWriteEvent()
	log.Printf("Written %d bytes at offset %d\n", len(buf), off)

	for rw.data.size > rw.MaxCacheSize {
		log.Printf("Suspend writer\n")
		err := rw.SuspendAndWaitForAwake(ctx)
		if err != nil {
			return err
		}
		log.Printf("Resume writer\n")
	}

	return nil
}

func (rw *ReliableWriterImpl) Complete(ctx context.Context) error {
	rw.mutex.Lock()
	isComplete := rw.isComplete
	isAborted := rw.isAborted
	if !isComplete && !isAborted {
		rw.isComplete = true
	}
	rw.mutex.Unlock()

	if isComplete {
		return errors.New("already completed")
	}
	if isAborted {
		return errors.New("was aborted")
	}

	rw.notifyWriteEvent()
	var err error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-rw.resultChan:
		log.Printf("Error received: %v\n", err)
	}
	if err != nil {
		return fmt.Errorf("writing failed: %w", err)
	}
	log.Println("Write operation completed.")

	if !rw.data.IsEmpty() {
		panic("Not all written")
	}
	log.Printf("Written at reliable writer: %d bytes\n", rw.writtenBytes)
	offset, err := rw.unreliableWriter.GetResumeOffset(ctx)
	log.Printf("Written at unreliable writer: %d bytes\n", offset)

	return nil
}

func (rw *ReliableWriterImpl) Abort(ctx context.Context) {
	log.Println("Aborting write operation...")
	rw.unreliableWriter.Abort(ctx)
	rw.mutex.Lock()
	rw.isComplete = false
	rw.isAborted = true
	rw.mutex.Unlock()
	rw.data = NewScatterGatherBuffer()
	rw.notifyWriteEvent()

	select {
	case <-ctx.Done():
	case _ = <-rw.resultChan:
	}

	log.Println("Write operation aborted.")
}

func (rw *ReliableWriterImpl) launchWriting(ctx context.Context) {
	go func() {
		defer close(rw.resultChan)
		defer fmt.Print("ReliableWriter: End launchWriting goroutine")
		for {
			select {
			case <-rw.writeEventsChan:
				log.Println("Handle writing event...")
				isFinished, err := rw.handleWriteEvents(ctx)
				if isFinished {
					log.Println("Finished writing.")
					rw.resultChan <- err
					return
				}

			case <-ctx.Done():
				log.Println("Writing goroutine shutting down.")
				rw.resultChan <- ctx.Err()
				return
			}
		}
	}()
}

func (rw *ReliableWriterImpl) handleWriteEvents(ctx context.Context) (isFinished bool, err error) {
	for !rw.data.IsEmpty() {
		rw.mutex.Lock()
		canBeLast := rw.isComplete
		rw.mutex.Unlock()

		if rw.isAborted {
			log.Println("Abort detected")
			return true, errors.New("aborted")
		}

		rw.mutex.Lock()
		var buf *ScatterGatherBuffer
		if canBeLast {
			buf, err = rw.data.TakeBytesSafely(0, rw.MaxChunkSize, 0, 1)
		} else {
			buf, err = rw.data.TakeBytesSafely(rw.MinChunkSize, rw.MaxChunkSize, rw.MinChunkSize, rw.MinChunkSize)
		}
		rw.mutex.Unlock()
		if err != nil {
			break
		}

		isLast := canBeLast && rw.data.IsEmpty()

		if rw.data.size <= rw.MaxCacheSize/2 {
			rw.WakeUp()
		}

		chunkBegin := int64(rw.offset)
		chunkEnd := chunkBegin + int64(buf.size)
		var written int64

		written, err = rw.attemptWriteWithRetries(ctx, buf.GetReader(), chunkBegin, chunkEnd, isLast)
		if err != nil {
			log.Println("Failed to write after retries:", err)
			rw.Abort(ctx)
			return true, err
		}
		rw.offset += uint64(written)

		if isLast {
			log.Println("Write complete. Writing goroutine shutting down.")
			return true, nil
		}
	}
	return false, nil
}

func (rw *ReliableWriterImpl) attemptWriteWithRetries(ctx context.Context, buf *ScatterGatherBuffer, chunkBegin, chunkEnd int64, isLast bool) (int64, error) {
	var totalWritten int64 = 0

	for attempt := 0; attempt < 10; attempt++ {
		if totalWritten == chunkEnd-chunkBegin {
			return totalWritten, nil
		}

		reader := buf.GetReader()

		log.Printf("Attempting to write from offset %d to %d\n", chunkBegin+totalWritten, chunkEnd)
		written, err := rw.unreliableWriter.WriteAt(ctx, chunkBegin+totalWritten, chunkEnd, reader, isLast)

		if err == nil {
			totalWritten += written
			return totalWritten, nil
		}

		log.Printf("Error writing to unreliable writer (attempt %d): %v\n", attempt+1, err)

		// todo: replace by RepairConn call
		//if err.HasTag(utils.TagNetwork) {
		//	log.Println("Rebuilding writer due to network error")
		//	if strings.Contains(err.Cause.Error(), "503") {
		//		writer, err := rw.unreliableWriterBuilder()
		//		if err != nil {
		//			return totalWritten, err
		//		}
		//		rw.unreliableWriter = writer
		//	}
		//}

		if !err.HasTag(utils.TagRetryable) {
			return totalWritten, err
		}

		currentOff, err := rw.unreliableWriter.GetResumeOffset(ctx)

		if err != nil {
			return totalWritten, err
		}

		amount := uint32(currentOff - chunkBegin - totalWritten)
		log.Printf("Dropping %d bytes\n", amount)
		buf.DropFirst(amount)
		totalWritten = currentOff - chunkBegin

		if ctx.Err() != nil {
			return totalWritten, ctx.Err()
		}
	}

	return totalWritten, errors.New("failed to write after 10 attempts")
}
