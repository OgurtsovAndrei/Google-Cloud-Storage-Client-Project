package writers

import (
	"context"
	"errors"
	"math/rand"
	"os"
	"sync"
	"time"
)

type UnreliableWriter interface {
	WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, buf []byte, off int64, isLast bool) (int64, error)
	GetResumeOffset(ctx context.Context) (int64, error)
	Abort(ctx context.Context)
}

type UnreliableLocalWriter struct {
	file      *os.File
	mu        sync.Mutex
	resumeOff int64
	isAborted bool
	filePath  string
}

func NewUnreliableLocalWriter(filePath string) (*UnreliableLocalWriter, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &UnreliableLocalWriter{
		file:      file,
		resumeOff: 0,
		isAborted: false,
		filePath:  filePath,
	}, nil
}

func (ulw *UnreliableLocalWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, buf []byte, off int64, isLast bool) (int64, error) {
	ulw.mu.Lock()
	defer ulw.mu.Unlock()

	if ulw.isAborted {
		return 0, errors.New("operation aborted")
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	if chunkEnd-chunkBegin != int64(len(buf)) {
		return 0, errors.New("buffer size does not match chunk range")
	}

	const batchSize = 4 * 1024 * 1024 // 4 MB
	var totalWritten int64 = 0

	for start := 0; start < len(buf); start += batchSize {
		end := start + batchSize
		if end > len(buf) {
			end = len(buf) // Ensure the last batch doesn't exceed the buffer size
		}

		if rand.Intn(100) == 42 {
			return totalWritten, errors.New("Bad Luck")
		}

		// Simulate random delay for each batch
		randomMs := rand.Intn(90) + 10
		time.Sleep(time.Duration(randomMs) * time.Millisecond)

		// Write the current batch
		_, err := ulw.file.WriteAt(buf[start:end], off+totalWritten)
		if err != nil {
			return totalWritten, err
		}

		totalWritten += int64(end - start)

		select {
		case <-ctx.Done():
			return totalWritten, ctx.Err()
		default:
		}
	}

	ulw.resumeOff = off + totalWritten

	if isLast {
		ulw.file.Close()
	}

	return totalWritten, nil
}

func (ulw *UnreliableLocalWriter) GetResumeOffset(ctx context.Context) (int64, error) {
	ulw.mu.Lock()
	defer ulw.mu.Unlock()

	if ulw.isAborted {
		return 0, errors.New("operation aborted")
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	return ulw.resumeOff, nil
}

func (ulw *UnreliableLocalWriter) Abort(ctx context.Context) {
	ulw.mu.Lock()
	defer ulw.mu.Unlock()

	select {
	case <-ctx.Done(): // If context is canceled, we respect it but still abort the operation.
	default:
	}

	ulw.isAborted = true
	if ulw.file != nil {
		ulw.file.Close()
	}
}
