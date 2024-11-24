package writers

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type ReliableWriter interface {
	WriteAt(ctx context.Context, buf []byte, off int64) error
	Complete(ctx context.Context) error
	Abort(ctx context.Context)
}

type ReliableWriterImpl struct {
	data             ScatterGatherBuffer
	writtenBytes     uint64
	mutex            sync.Mutex
	MaxCacheSize     uint32
	MaxChunkSize     uint32
	isComplete       bool
	suspendChan      chan struct{}
	writeEventsChan  chan struct{}
	unreliableWriter UnreliableWriter
}

func NewReliableWriterImpl(ctx context.Context, writer UnreliableWriter) *ReliableWriterImpl {
	rw := &ReliableWriterImpl{
		data:             *NewScatterGatherBuffer(),
		isComplete:       false,
		suspendChan:      make(chan struct{}, 1),
		writeEventsChan:  make(chan struct{}, 1),
		unreliableWriter: writer,
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

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
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

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	rw.isComplete = true
	rw.notifyWriteEvent()
	fmt.Println("Write operation completed.")
	return nil
}

func (rw *ReliableWriterImpl) Abort(ctx context.Context) {
	rw.unreliableWriter.Abort(ctx)
	rw.isComplete = false
	rw.data = *NewScatterGatherBuffer()
	fmt.Println("Write operation aborted.")
}

func (rw *ReliableWriterImpl) launchWriting(ctx context.Context) {
	var bytesWritten int64 = 0
	go func() {
		for {
			select {
			case <-rw.writeEventsChan:
				if rw.handleWriteEvents(bytesWritten, ctx) {
					return
				}

			case <-ctx.Done():
				fmt.Println("Writing goroutine shutting down.")
				return
			}
		}
	}()
}

func (rw *ReliableWriterImpl) handleWriteEvents(bytesWritten int64, ctx context.Context) bool {
	rw.mutex.Lock()
	isLast := rw.isComplete
	rw.mutex.Unlock()

	for !rw.data.IsEmpty() {
		rw.mutex.Lock()
		buf, err := rw.data.TakeBytes(rw.MaxChunkSize)
		rw.mutex.Unlock()

		if err != nil {
			fmt.Println("Error taking bytes:", err)
			break
		}

		if rw.data.size <= rw.MaxCacheSize/2 {
			rw.WakeUp()
		}

		chunkBegin := bytesWritten
		chunkEnd := bytesWritten + int64(len(buf.ToBytes()))

		written, err := rw.attemptWriteWithRetries(ctx, buf.ToBytes(), bytesWritten, chunkBegin, chunkEnd, isLast)
		bytesWritten += written

		if err != nil {
			fmt.Println("Failed to write after retries:", err)
			rw.Abort(ctx)
		}

		if isLast {
			fmt.Println("Write complete. Writing goroutine shutting down.")
			return true
		}
	}
	return false
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
