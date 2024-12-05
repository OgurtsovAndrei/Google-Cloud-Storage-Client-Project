package writers

import (
	"errors"
	"github.com/gammazero/deque"
	"io"
)

type ScatterGatherBuffer struct {
	buffer deque.Deque[[]byte]
	size   uint32
}

func NewScatterGatherBuffer() ScatterGatherBuffer {
	return ScatterGatherBuffer{
		buffer: deque.Deque[[]byte]{},
		size:   0,
	}
}

func (sgb *ScatterGatherBuffer) AddBytes(buffer []byte) {
	if len(buffer) == 0 {
		return
	}
	sgb.buffer.PushBack(buffer)
	sgb.size += uint32(len(buffer)) // Update the total size
}

func (sgb *ScatterGatherBuffer) AddSCG(anotherSGB *ScatterGatherBuffer) {
	for i := 0; i < anotherSGB.buffer.Len(); i++ {
		sgb.AddBytes(anotherSGB.buffer.At(i))
	}
}

func (sgb *ScatterGatherBuffer) TakeBytesSafely(minSize uint32, maxSize uint32, leftAtLeast uint32, alignment uint32) (*ScatterGatherBuffer, error) {
	if sgb.size < minSize+leftAtLeast {
		return &ScatterGatherBuffer{}, errors.New("buffer is not big enough")
	}

	resultSGB := NewScatterGatherBuffer()
	accumulatedBuffer := NewScatterGatherBuffer()

	for !sgb.IsEmpty() {
		front := sgb.buffer.PopFront()
		sgb.size -= uint32(len(front))
		accumulatedBuffer.AddBytes(front)

		takeLimit := min(sgb.size+accumulatedBuffer.size-leftAtLeast, maxSize-resultSGB.size)
		takeLimit = takeLimit - (takeLimit % alignment)
		if takeLimit == 0 {
			break
		}

		if accumulatedBuffer.size >= alignment {
			takeAmount := accumulatedBuffer.size - (accumulatedBuffer.size % alignment)
			accumulatedBuffer.MoveBytesFromFrontToEnd(&resultSGB, min(takeAmount, takeLimit))
		}
	}

	accumulatedBuffer.MoveBytesFromEndToFront(sgb, accumulatedBuffer.size)

	if resultSGB.size < minSize || resultSGB.size > maxSize || resultSGB.size%alignment != 0 {
		panic("Chunk mismatch")
	}
	if sgb.size < leftAtLeast {
		panic("Fuck Fuck Fuck")
	}
	if accumulatedBuffer.size != 0 {
		panic("Lose data")
	}
	sgb.checkSize()
	resultSGB.checkSize()

	return &resultSGB, nil
}

func (sgb *ScatterGatherBuffer) checkSize() {
	sizeFromBuff := uint32(0)

	for i := 0; i < sgb.buffer.Len(); i++ {
		chunk := sgb.buffer.At(i)
		sizeFromBuff += uint32(len(chunk))
	}

	if sizeFromBuff != sgb.size {
		panic("Size mismatch detected in ScatterGatherBuffer! Calculated size does not match stored size.")
	}
}

func (sgb *ScatterGatherBuffer) MoveBytesFromFrontToEnd(targetBuffer *ScatterGatherBuffer, amount uint32) {
	var moved uint32

	for moved < amount && sgb.buffer.Len() > 0 {
		front := sgb.buffer.PopFront()
		frontLen := uint32(len(front))

		if moved+frontLen <= amount {
			targetBuffer.AddBytes(front)
			moved += frontLen
		} else {
			remaining := amount - moved
			targetBuffer.AddBytes(front[:remaining])
			sgb.buffer.PushFront(front[remaining:])
			moved += remaining
		}
	}

	sgb.size -= moved
}

func (sgb *ScatterGatherBuffer) MoveBytesFromEndToFront(targetBuffer *ScatterGatherBuffer, amount uint32) {
	var moved uint32

	for moved < amount && sgb.buffer.Len() > 0 {
		rear := sgb.buffer.PopBack()
		rearLen := uint32(len(rear))

		if moved+rearLen <= amount {
			targetBuffer.AddBytesToFront(rear)
			moved += rearLen
		} else {
			remaining := amount - moved
			targetBuffer.AddBytesToFront(rear[rearLen-remaining:])
			sgb.buffer.PushBack(rear[:rearLen-remaining])
			moved += remaining
		}
	}

	sgb.size -= moved
}

func (sgb *ScatterGatherBuffer) AddBytesToFront(data []byte) {
	sgb.buffer.PushFront(data)
	sgb.size += uint32(len(data))
}

func (sgb *ScatterGatherBuffer) IsEmpty() bool {
	return sgb.size == 0
}

func (sgb *ScatterGatherBuffer) GetPipeReader() *io.PipeReader {
	reader, writer := io.Pipe()

	go func() {
		defer writer.Close()

		for i := 0; i < sgb.buffer.Len(); i++ {
			chunk := sgb.buffer.At(i)
			_, err := writer.Write(chunk)
			if err != nil {
				_ = writer.CloseWithError(err)
				return
			}
		}
	}()

	return reader
}

func (sgb *ScatterGatherBuffer) DropFirst(amount uint32) {
	var dropped uint32

	for dropped < amount && sgb.buffer.Len() > 0 {
		front := sgb.buffer.PopFront()
		frontLen := uint32(len(front))

		if dropped+frontLen <= amount {
			dropped += frontLen
		} else {
			remaining := amount - dropped
			sgb.buffer.PushFront(front[remaining:])
			dropped += remaining
		}
	}

	sgb.size -= dropped
}
