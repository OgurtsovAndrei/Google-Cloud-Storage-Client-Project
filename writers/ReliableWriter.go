package writers

import (
	"awesomeProject/retrier"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
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
	retryConfig      retrier.RetryConfig
	pendingWrites    []writeRequest
}

type writeRequest struct {
	offset uint64
	data   []byte
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
		retryConfig: retrier.RetryConfig{
			MaxRetries:      5,
			InitialInterval: 1 * time.Second,
			MaxInterval:     10 * time.Second,
			Multiplier:      2.0,
			MaxJitter:       500 * time.Millisecond,
		},
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

	rw.mutex.Lock()
	rw.pendingWrites = append(rw.pendingWrites, writeRequest{
		offset: uint64(off),
		data:   buf,
	})

	sort.Slice(rw.pendingWrites, func(i, j int) bool {
		return rw.pendingWrites[i].offset < rw.pendingWrites[j].offset
	})

	for len(rw.pendingWrites) > 0 {
		next := rw.pendingWrites[0]
		if next.offset != rw.writtenBytes {
			break
		}

		rw.data.AddBytes(next.data)
		rw.writtenBytes += uint64(len(next.data))
		rw.pendingWrites = rw.pendingWrites[1:]
	}
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

	// Add retry for the final validation
	err := retrier.RetryWithBackoff(ctx, "validate_completion", rw.retryConfig, func(attempt int) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-rw.resultChan:
			if err != nil {
				return retrier.NewRetryableError(err, true, "validate_completion", attempt)
			}
			return nil
		}
	})

	if err != nil {
		return fmt.Errorf("writing failed after retries: %w", err)
	}

	if !rw.data.IsEmpty() {
		panic("Not all written")
	}

	//retry for final offset
	var finalOffset int64
	err = retrier.RetryWithBackoff(ctx, "get_final_offset", rw.retryConfig, func(attempt int) error {
		var err error
		finalOffset, err = rw.unreliableWriter.GetResumeOffset(ctx)
		if err != nil {
			return retrier.NewRetryableError(err, true, "get_final_offset", attempt)
		}
		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to get final offset: %w", err)
	}

	fmt.Printf("Written at reliable writer: %d bytes\n", rw.writtenBytes)
	fmt.Printf("Written at unreliable writer: %d bytes\n", finalOffset)

	return nil
}

func (rw *ReliableWriterImpl) Abort(ctx context.Context) {
	// retry for abort
	_ = retrier.RetryWithBackoff(ctx, "abort_operation", rw.retryConfig, func(attempt int) error {
		rw.unreliableWriter.Abort(ctx)
		return nil
	})

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

		written, err := rw.attemptWriteWithRetries(ctx, buf, chunkBegin, chunkEnd, isLast)
		if err != nil {
			var retryErr *retrier.RetryableError
			if errors.As(err, &retryErr) && retryErr.Retriable {
				fmt.Printf("Retryable error occurred, will retry: %v\n", err)
				continue
			}
			fmt.Println("Non-retryable error occurred:", err)
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

func (rw *ReliableWriterImpl) attemptWriteWithRetries(ctx context.Context, buf *ScatterGatherBuffer, chunkBegin, chunkEnd int64, isLast bool) (int64, error) {
	var totalWritten int64 = 0

	// deep copy the buffer for retries
	originalData := &ScatterGatherBuffer{
		size: buf.size,
	}
	for i := 0; i < buf.buffer.Len(); i++ {
		chunk := buf.buffer.At(i)
		chunkCopy := make([]byte, len(chunk))
		copy(chunkCopy, chunk)
		originalData.buffer.PushBack(chunkCopy)
	}

	err := retrier.RetryWithBackoff(ctx, "write_chunk", rw.retryConfig, func(attempt int) error {
		remainingData := &ScatterGatherBuffer{
			size: originalData.size,
		}
		for i := 0; i < originalData.buffer.Len(); i++ {
			chunk := originalData.buffer.At(i)
			chunkCopy := make([]byte, len(chunk))
			copy(chunkCopy, chunk)
			remainingData.buffer.PushBack(chunkCopy)
		}

		if totalWritten > 0 {
			remainingData.DropFirst(uint32(totalWritten))
		}
		reader := remainingData.GetPipeReader()

		if attempt > 0 {
			fmt.Printf("[Retry] 🔄 Attempt #%d for chunk [%d-%d]\n", attempt+1, chunkBegin+totalWritten, chunkEnd)
		}

		written, err := rw.unreliableWriter.WriteAt(ctx, chunkBegin+totalWritten, chunkEnd, reader, isLast)
		if err != nil {
			fmt.Printf("[Retry] ❌ Failed attempt #%d: %v\n", attempt+1, err)

			shouldRetry := retrier.IsRetryableError(err)
			return retrier.NewRetryableError(err, shouldRetry, "write_chunk", attempt)
		}

		totalWritten += written
		buf.DropFirst(uint32(written))

		if totalWritten < chunkEnd-chunkBegin {
			fmt.Printf("[Retry] ⚠️ Partial write in attempt #%d: %d/%d bytes\n",
				attempt+1, totalWritten, chunkEnd-chunkBegin)
			return retrier.NewRetryableError(
				fmt.Errorf("partial write: %d/%d bytes", totalWritten, chunkEnd-chunkBegin),
				true,
				"write_chunk",
				attempt,
			)
		}

		if attempt > 0 {
			fmt.Printf("[Retry] ✅ Success on attempt #%d\n", attempt+1)
		}
		return nil
	})

	if err != nil {
		fmt.Printf("[Retry] 🛑 Error after all retries: %v\n", err)
		return totalWritten, err
	}

	return totalWritten, nil
}
