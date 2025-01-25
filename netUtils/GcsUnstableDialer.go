package netUtils

import (
	"context"
	"net"
)

type GcsUnstableDialer struct {
	threshold int64
}

func NewUnstableDialer(threshold int64) *GcsUnstableDialer {
	return &GcsUnstableDialer{
		threshold: threshold,
	}
}

func (d *GcsUnstableDialer) Dial(network, address string) (net.Conn, error) {
	baseConn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return NewGcsUnstableConn(NewGcsThreadSafeConn(baseConn), d.threshold), nil
}

func (d *GcsUnstableDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	baseConn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return NewGcsUnstableConn(NewGcsThreadSafeConn(baseConn), d.threshold), nil
}
