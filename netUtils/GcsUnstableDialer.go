package netUtils

import (
	"context"
	"net"
	"net/http"
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
	return NewGcsUnstableConn(baseConn, d.threshold), nil
}

func (d *GcsUnstableDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	baseConn, err := (&net.Dialer{}).DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return NewGcsUnstableConn(baseConn, d.threshold), nil
}

func NewUnstableHttpClient(threshold int64) *http.Client {
	unstableDialer := NewUnstableDialer(threshold)
	transport := &http.Transport{
		DialContext: unstableDialer.DialContext,
	}

	return &http.Client{
		Transport: transport,
	}
}
