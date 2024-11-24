package writers

import (
	"errors"
	"github.com/gammazero/deque"
)

type ScatterGatherBuffer struct {
	buffer deque.Deque[[]byte]
	size   uint32
}

func NewScatterGatherBuffer() *ScatterGatherBuffer {
	return &ScatterGatherBuffer{
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

func (sgb *ScatterGatherBuffer) TakeBytes(number uint32) (*ScatterGatherBuffer, error) {
	if sgb.buffer.Len() == 0 {
		return sgb, errors.New("buffer is empty")
	}

	if number > sgb.size {
		return sgb, errors.New("buffer is not big enough")
	}

	resultSGB := NewScatterGatherBuffer()
	var collected uint32

	for collected < number && sgb.buffer.Len() > 0 {
		front := sgb.buffer.PopFront()
		frontLen := uint32(len(front))
		if collected+frontLen <= number { // Take the entire chunk
			resultSGB.AddBytes(front)
			collected += frontLen
			sgb.size -= frontLen
		} else { // Take only the necessary part of the chunk
			remaining := number - collected
			resultSGB.AddBytes(front[:remaining])
			sgb.buffer.PushFront(front[remaining:])
			collected += remaining
			sgb.size -= remaining
		}
	}

	return resultSGB, nil
}

func (sgb *ScatterGatherBuffer) Size() uint32 {
	return sgb.size
}

func (sgb *ScatterGatherBuffer) IsEmpty() bool {
	return sgb.size == 0
}

func (sgb *ScatterGatherBuffer) ToBytes() []byte {
	result := make([]byte, sgb.size)
	offset := 0

	for i := 0; i < sgb.buffer.Len(); i++ {
		chunk := sgb.buffer.At(i)
		copy(result[offset:], chunk)
		offset += len(chunk)
	}

	return result
}
