package netUtils

import "net"

// GcsListener is a custom net.Listener implementation that wraps GcsSafeConn.
type GcsListener struct {
	listener net.Listener
}

// NewGcsSafeListener creates a new GcsListener.
func NewGcsSafeListener(listener net.Listener) *GcsListener {
	if listener == nil {
		panic("listener cannot be nil")
	}
	return &GcsListener{
		listener: listener,
	}
}

// Accept waits for and returns the next connection to the listener, wrapped in GcsSafeConn.
func (l *GcsListener) Accept() (net.Conn, error) {
	conn, err := l.listener.Accept()
	if err != nil {
		return nil, err
	}
	return NewGcsThreadSafeConn(conn), nil
}

// Close closes the listener.
func (l *GcsListener) Close() error {
	return l.listener.Close()
}

// Addr returns the listener's network address.
func (l *GcsListener) Addr() net.Addr {
	return l.listener.Addr()
}
