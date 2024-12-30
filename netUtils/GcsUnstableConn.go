package netUtils

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

var (
	cancelConnProba = 32 // p = 1 / x
)

type GcsUnstableConn struct {
	conn       net.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	closeMx    sync.Mutex
	closed     bool
	threshold  int64
	totalBytes int64
}

func NewGcsUnstableConn(baseConn net.Conn, threshold int64) *GcsUnstableConn {
	ctx, cancel := context.WithCancel(context.Background())
	wrapper := &GcsUnstableConn{
		conn:      baseConn,
		ctx:       ctx,
		cancel:    cancel,
		threshold: threshold,
	}
	return wrapper
}

func (e *GcsUnstableConn) checkThreshold(n int64) {
	e.totalBytes += n
	if e.totalBytes >= e.threshold {
		e.closeMx.Lock()
		if !e.closed {
			if rand.Intn(cancelConnProba) == 0 {
				fmt.Println("Injection err to connection")
				e.cancel()
				e.conn.Close()
				e.closed = true
			}
			e.totalBytes = 0
		}
		e.closeMx.Unlock()
	}
}

// Interface Impl

func (e *GcsUnstableConn) Read(b []byte) (int, error) {
	select {
	case <-e.ctx.Done():
		return 0, errors.New("connection closed due to injected error")
	default:
		n, err := e.conn.Read(b)
		e.checkThreshold(int64(n))
		return n, err
	}
}

func (e *GcsUnstableConn) Write(b []byte) (int, error) {
	select {
	case <-e.ctx.Done():
		return 0, errors.New("connection closed due to injected error")
	default:
		n, err := e.conn.Write(b)
		e.checkThreshold(int64(n))
		return n, err
	}
}

func (e *GcsUnstableConn) Close() error {
	e.closeMx.Lock()
	defer e.closeMx.Unlock()
	if e.closed {
		return nil
	}
	e.cancel()
	e.closed = true
	return e.conn.Close()
}

func (e *GcsUnstableConn) LocalAddr() net.Addr {
	return e.conn.LocalAddr()
}

func (e *GcsUnstableConn) RemoteAddr() net.Addr {
	return e.conn.RemoteAddr()
}

func (e *GcsUnstableConn) SetDeadline(t time.Time) error {
	return e.conn.SetDeadline(t)
}

func (e *GcsUnstableConn) SetReadDeadline(t time.Time) error {
	return e.conn.SetReadDeadline(t)
}

func (e *GcsUnstableConn) SetWriteDeadline(t time.Time) error {
	return e.conn.SetWriteDeadline(t)
}
