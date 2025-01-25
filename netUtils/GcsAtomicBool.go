package netUtils

import (
	"sync/atomic"
)

type AtomicBool struct {
	flag int32
}

// NewAtomicBool creates a new AtomicBool with an initial value.
func NewAtomicBool(initial bool) *AtomicBool {
	var value int32
	if initial {
		value = 1
	}
	return &AtomicBool{flag: value}
}

// Set sets the value of the atomic bool.
func (b *AtomicBool) Set(value bool) {
	var intValue int32 = 0
	if value {
		intValue = 1
	}
	atomic.StoreInt32(&b.flag, intValue)
}

// Get returns the current value of the atomic bool.
func (b *AtomicBool) Get() bool {
	return atomic.LoadInt32(&b.flag) == 1
}

// CompareAndSwap performs an atomic compare-and-swap operation.
func (b *AtomicBool) CompareAndSwap(old, new bool) bool {
	var oldValue, newValue int32
	if old {
		oldValue = 1
	}
	if new {
		newValue = 1
	}
	return atomic.CompareAndSwapInt32(&b.flag, oldValue, newValue)
}
