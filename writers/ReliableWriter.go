package writers

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type ReliableWriterConfig struct {
	MaxCacheSize uint32
	MinChunkSize uint32
	MaxChunkSize uint32
}

type ReliableWriterImpl struct {
	data             ScatterGatherBuffer
	writtenBytes     uint64
	offset           uint64
	mutex            sync.Mutex
	MaxCacheSize     uint32
	MinChunkSize     uint32
	MaxChunkSize     uint32
	isComplete       bool
	isAborted        bool
	suspendChan      chan struct{}
	writeEventsChan  chan struct{}
	unreliableWriter UnreliableWriter
	resultChan       chan error
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
	fmt.Printf("Written %d bytes at offset %d\n", len(buf), off)

	for rw.data.size > rw.MaxCacheSize {
		fmt.Printf("Suspend writer\n")
		err := rw.SuspendAndWaitForAwake(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("Resume writer\n")
	}

	return nil
}

func (rw *ReliableWriterImpl) Complete(ctx context.Context) error {
	if rw.isComplete {
		return errors.New("already completed")
	}

	rw.mutex.Lock()
	rw.isComplete = true
	rw.mutex.Unlock()
	rw.notifyWriteEvent()
	var err error
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-rw.resultChan:
	}
	if err != nil {
		return fmt.Errorf("writing failed: %w", err)
	}
	fmt.Println("Write operation completed.")

	if !rw.data.IsEmpty() {
		panic("Not all written")
	}
	fmt.Printf("Written at reliable writer: %d bytes\n", rw.writtenBytes)
	offset, err := rw.unreliableWriter.GetResumeOffset(ctx)
	fmt.Printf("Written at unreliable writer: %d bytes\n", offset)

	return nil
}

func (rw *ReliableWriterImpl) Abort(ctx context.Context) {
	rw.unreliableWriter.Abort(ctx)
	rw.isComplete = false
	rw.isAborted = true
	rw.data = NewScatterGatherBuffer()
	rw.notifyWriteEvent() // To resume launch
	fmt.Println("Write operation aborted.")
}

func (rw *ReliableWriterImpl) launchWriting(ctx context.Context) {
	go func() {
		defer close(rw.resultChan)
		defer fmt.Print("End launch goroutine")
		for {
			select {
			case <-rw.writeEventsChan:
				fmt.Println("Handle writing event...")
				isFinished, err := rw.handleWriteEvents(ctx)
				if isFinished {
					fmt.Println("Finished writing.")
					rw.resultChan <- err
					return
				}

			case <-ctx.Done():
				fmt.Println("Writing goroutine shutting down.")
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
			fmt.Println("Abort detected")
			return true, errors.New("aborted")
		}

		rw.mutex.Lock()
		var buf ScatterGatherBuffer
		if canBeLast {
			buf, err = rw.data.TakeBytes(0, rw.MaxChunkSize, 0, 1)
		} else {
			buf, err = rw.data.TakeBytes(rw.MinChunkSize, rw.MaxChunkSize, rw.MinChunkSize, rw.MinChunkSize)
		}
		rw.mutex.Unlock()
		if err != nil {
			break
		}

		isLast := canBeLast && rw.data.IsEmpty()

		if rw.data.size <= rw.MaxCacheSize/2 {
			rw.WakeUp()
		}

		chunkBegin := int64(0)
		chunkEnd := int64(buf.size)

		written, err := rw.attemptWriteWithRetries(ctx, buf.ToBytes(), int64(rw.offset), chunkBegin, chunkEnd, isLast)
		if err != nil {
			fmt.Println("Failed to write after retries:", err)
			rw.Abort(ctx)
			return true, err
		}
		rw.offset += uint64(written)

		if isLast {
			fmt.Println("Write complete. Writing goroutine shutting down.")
			return true, nil
		}
	}
	return false, nil
}

func (rw *ReliableWriterImpl) attemptWriteWithRetries(ctx context.Context, buf []byte, totalOffset int64, chunkBegin, chunkEnd int64, isLast bool) (int64, error) {
	var totalWritten int64 = 0
	remaining := buf

	for attempt := 0; attempt < 3; attempt++ {
		written, err := rw.unreliableWriter.WriteAt(ctx, chunkBegin+totalWritten, chunkEnd, remaining, totalOffset+totalWritten, isLast)
		totalWritten += written

		if err == nil {
			return totalWritten, nil
		}

		fmt.Printf("Error writing to unreliable writer (attempt %d): %v\n", attempt+1, err)

		if written < int64(len(remaining)) {
			remaining = remaining[written:]
		}

		if ctx.Err() != nil {
			return totalWritten, ctx.Err()
		}
	}

	return totalWritten, errors.New("failed to write after 3 attempts")
}
