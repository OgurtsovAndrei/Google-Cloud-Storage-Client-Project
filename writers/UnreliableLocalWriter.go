package writers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"
)

type Error struct {
	Code  string
	Msg   string
	Cause error
}

// UnreliableWriter Not thread safe, so all methods have to be called from same thread
type UnreliableWriter interface {
	WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader io.Reader, isLast bool) (int64, error)
	GetResumeOffset(ctx context.Context) (int64, error)
	Abort(ctx context.Context)
}

type UnreliableLocalWriter struct {
	file      *os.File
	resumeOff int64
	isAborted bool
	filePath  string
}

func NewUnreliableLocalWriter(filePath string) (*UnreliableLocalWriter, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
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

func (ulw *UnreliableLocalWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader io.Reader, isLast bool) (int64, error) {
	if chunkBegin != ulw.resumeOff {
		panic(fmt.Sprintf("WriteAt called on chunkBegin %d, but resumeOff is %d", chunkBegin, ulw.resumeOff))
	}

	if ulw.isAborted {
		return 0, errors.New("operation aborted")
	}

	size := chunkEnd - chunkBegin
	const batchSize = 4 * 1024 * 1024 // 4 MB
	var totalWritten int64 = 0
	buffer := make([]byte, batchSize)

	for totalWritten < size {
		remaining := size - totalWritten
		toRead := batchSize
		if remaining < int64(toRead) {
			toRead = int(remaining) // Adjust for the last partial batch
		}

		// Read the next batch from the reader
		n, err := reader.Read(buffer[:toRead])
		if err != nil && err != io.EOF {
			ulw.resumeOff = chunkBegin + totalWritten
			return totalWritten, fmt.Errorf("failed to read from reader: %w", err)
		}

		if n == 0 {
			break // EOF or no more data to read
		}

		if rand.Intn(100) == 42 {
			ulw.resumeOff = chunkBegin + totalWritten
			return totalWritten, errors.New("Bad Luck")
		}

		// Simulate random delay for each batch
		randomMs := rand.Intn(40) + 10
		time.Sleep(time.Duration(randomMs) * time.Millisecond)

		_, err = ulw.file.WriteAt(buffer[:n], chunkBegin+totalWritten)
		if err != nil {
			ulw.resumeOff = chunkBegin + totalWritten
			return totalWritten, err
		}

		totalWritten += int64(n)
	}

	ulw.resumeOff = chunkBegin + totalWritten

	if isLast {
		ulw.file.Close()
	}

	return totalWritten, nil
}

func (ulw *UnreliableLocalWriter) GetResumeOffset(_ context.Context) (int64, error) {
	if ulw.isAborted {
		return 0, errors.New("operation aborted")
	}
	return ulw.resumeOff, nil
}

func (ulw *UnreliableLocalWriter) Abort(_ context.Context) {
	ulw.isAborted = true
	if ulw.file != nil {
		ulw.file.Close()
	}
}
