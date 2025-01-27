package utils

import (
	"context"
	"net"
	"net/http"
	"time"
)

type FaultyConnection struct {
	net.Conn
	injector *NetworkFaultInjector
}

func (fc *FaultyConnection) Read(b []byte) (n int, err error) {
	if fc.injector.ShouldInjectError() {
		return 0, fc.injector.GetError()
	}
	return fc.Conn.Read(b)
}

func (fc *FaultyConnection) Write(b []byte) (n int, err error) {
	if fc.injector.ShouldInjectError() {
		return 0, fc.injector.GetError()
	}
	return fc.Conn.Write(b)
}

type FaultyDialer struct {
	dialer   *net.Dialer
	injector *NetworkFaultInjector
}

func NewFaultyDialer(injector *NetworkFaultInjector) *FaultyDialer {
	return &FaultyDialer{
		dialer: &net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		},
		injector: injector,
	}
}

func (fd *FaultyDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	conn, err := fd.dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	return &FaultyConnection{
		Conn:     conn,
		injector: fd.injector,
	}, nil
}

type FaultyTransport struct {
	transport *http.Transport
	injector  NetworkFaultInjector
}

func NewFaultyTransport(injector *NetworkFaultInjector) *FaultyTransport {
	dialer := NewFaultyDialer(injector)
	transport := &http.Transport{
		DialContext:         dialer.DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}
	return &FaultyTransport{transport: transport, injector: *injector}
}

func (ft *FaultyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return ft.transport.RoundTrip(req)
}
