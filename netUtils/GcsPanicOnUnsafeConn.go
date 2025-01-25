package netUtils

import (
	"net"
	"time"
)

type GcsSafeConn struct {
	baseConn  net.Conn
	busyRead  AtomicBool
	busyWrite AtomicBool
}

// NewGcsThreadSafeConn creates a new GcsSafeConn with the provided net.Conn and threshold.
func NewGcsThreadSafeConn(baseConn net.Conn) *GcsSafeConn {
	if baseConn == nil {
		panic("baseConn cannot be nil")
	}
	return &GcsSafeConn{
		baseConn: baseConn,
	}
}

// Read ensures that only one thread can perform a Read operation at a time.
func (c *GcsSafeConn) Read(b []byte) (n int, err error) {
	if !c.busyRead.CompareAndSwap(false, true) {
		panic("unsafe concurrent Read detected")
	}
	n, err = c.baseConn.Read(b)
	if !c.busyRead.CompareAndSwap(true, false) {
		panic("unexpected state: failed to reset busy flag")
	}
	return
}

// Write ensures that only one thread can perform a Write operation at a time.
func (c *GcsSafeConn) Write(b []byte) (n int, err error) {
	if !c.busyWrite.CompareAndSwap(false, true) {
		panic("unsafe concurrent Write detected")
	}
	n, err = c.baseConn.Write(b)
	if !c.busyWrite.CompareAndSwap(true, false) {
		panic("unexpected state: failed to reset busy flag")
	}
	return
}

// Close closes the underlying connection.
func (c *GcsSafeConn) Close() error {
	return c.baseConn.Close()
}

// LocalAddr returns the local network address.
func (c *GcsSafeConn) LocalAddr() net.Addr {
	return c.baseConn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *GcsSafeConn) RemoteAddr() net.Addr {
	return c.baseConn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines associated with the connection.
func (c *GcsSafeConn) SetDeadline(t time.Time) error {
	return c.baseConn.SetDeadline(t)
}

// SetReadDeadline sets the deadline for future Read calls.
func (c *GcsSafeConn) SetReadDeadline(t time.Time) error {
	return c.baseConn.SetReadDeadline(t)
}

// SetWriteDeadline sets the deadline for future Write calls.
func (c *GcsSafeConn) SetWriteDeadline(t time.Time) error {
	return c.baseConn.SetWriteDeadline(t)
}
