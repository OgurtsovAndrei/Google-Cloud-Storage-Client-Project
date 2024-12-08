package utils

import (
	"errors"
	"io"
	"sync"
)

type BuildableBuffer struct {
	mutex   sync.Mutex
	parts   map[uint32][]byte
	size    uint32
	written uint32
	readOff uint32
}

func NewBuildableBuffer(size uint32) *BuildableBuffer {
	return &BuildableBuffer{
		size:  size,
		parts: make(map[uint32][]byte),
	}
}

func (buffer *BuildableBuffer) WriteToOffset(offset uint32, data []byte) error {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	if _, exists := buffer.parts[offset]; exists {
		return errors.New("element at the specified offset already exists")
	}

	buffer.parts[offset] = data
	buffer.written += uint32(len(data))
	return nil
}

func (buffer *BuildableBuffer) Read(p []byte) (n int, err error) {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()

	if buffer.readOff >= buffer.size {
		return 0, io.EOF
	}

	for buffer.readOff < buffer.size && n < len(p) {
		part, exists := buffer.parts[buffer.readOff]
		if exists {
			toCopy := len(part)
			if n+toCopy > len(p) {
				toCopy = len(p) - n
			}

			copy(p[n:], part[:toCopy])
			n += toCopy

			if toCopy < len(part) {
				buffer.parts[buffer.readOff+uint32(toCopy)] = part[toCopy:]
			}

			delete(buffer.parts, buffer.readOff)
			buffer.readOff += uint32(toCopy)
		} else {
			break
		}
	}

	if n == 0 && buffer.readOff < buffer.size {
		return 0, nil
	}

	return n, nil
}

func (buffer *BuildableBuffer) IsFull() bool {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	return buffer.written == buffer.size
}
