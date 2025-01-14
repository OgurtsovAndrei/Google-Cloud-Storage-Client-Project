package utils

import (
	"cloud.google.com/go/storage"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/net/http2"
	"io"
	"math/rand"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

type ProgressReader struct {
	reader     io.Reader
	bar        *progressbar.ProgressBar
	totalBytes int64
}

func NewProgressReader(reader io.Reader, size int64, description string) *ProgressReader {
	bar := progressbar.NewOptions64(
		size,
		progressbar.OptionSetDescription(description),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(30),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionSetRenderBlankState(true),
	)
	return &ProgressReader{reader: reader, bar: bar, totalBytes: size}
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		_ = pr.bar.Add(n)
	}
	return n, err
}

func GenerateTestPattern(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*17 + 41) % 251)
	}
	return data
}

func VerifyChunkPositions(t *testing.T, data []byte) {
	chunkSize := 1024 * 1024 // 1MB chunks
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunk := data[offset:end]

		// verify position using the same pattern
		for i := 0; i < len(chunk); i++ {
			expected := byte(((offset+i)*17 + 41) % 251)
			if chunk[i] != expected {
				t.Errorf("Chunk misplaced at offset %d+%d: got %d, want %d",
					offset, i, chunk[i], expected)
				break
			}
		}
	}
}

type NetworkFaultInjector struct {
	faultRatio    float64
	faults        []error
	rnd           *rand.Rand
	injectedCount int
	injectedMap   map[string]bool // Track which errors were injected
}

func NewNetworkFaultInjector(faultRatio float64, faults []error) *NetworkFaultInjector {
	return &NetworkFaultInjector{
		faultRatio:  faultRatio,
		faults:      faults,
		rnd:         rand.New(rand.NewSource(time.Now().UnixNano())),
		injectedMap: make(map[string]bool),
	}
}

func (n *NetworkFaultInjector) AllErrorsInjected() bool {
	return len(n.injectedMap) == len(n.faults)
}

func (n *NetworkFaultInjector) ShouldInjectError() bool {
	shouldInject := n.rnd.Float64() < n.faultRatio
	if shouldInject {
		n.injectedCount++
		fmt.Printf("\n[Fault Injector] 💉 Injecting error #%d\n", n.injectedCount)
	}
	return shouldInject
}

func (n *NetworkFaultInjector) GetError() error {
	// If not all errors injected, prioritize uninjected ones
	if !n.AllErrorsInjected() {
		// Get uninjected errors
		var uninjected []error
		for _, err := range n.faults {
			if !n.injectedMap[fmt.Sprintf("%T:%v", err, err)] {
				uninjected = append(uninjected, err)
			}
		}
		err := uninjected[n.rnd.Intn(len(uninjected))]
		n.injectedMap[fmt.Sprintf("%T:%v", err, err)] = true
		fmt.Printf("[Fault Injector] Generated new error: %v\n", err)
		return err
	}

	// All errors injected at least once, random selection
	err := n.faults[n.rnd.Intn(len(n.faults))]
	fmt.Printf("[Fault Injector] Generated repeated error: %v\n", err)
	return err
}

func DownloadFromGCS(ctx context.Context, bucket, object string) ([]byte, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	obj := client.Bucket(bucket).Object(object)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	pr := NewProgressReader(r, r.Size(), "Downloading")
	return io.ReadAll(pr)
}

var NetworkErrors = []error{
	// TLS errors
	tls.AlertError(20), //alertBadRecordMAC
	tls.AlertError(10), //alertUnexpectedMessage
	tls.AlertError(40), //alertHandshakeFailure
	tls.AlertError(80), //alertInternalError
	tls.AlertError(70), //alertProtocolVersion
	tls.AlertError(71), //alertInsufficientSecurity
	tls.AlertError(50), //alertDecodeError
	tls.AlertError(22), //alertRecordOverflow

	// HTTP/2 errors
	&http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeProtocol},        // protocol errors
	&http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeInternal},        // server internal errors
	&http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeFlowControl},     // flow control issues
	&http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeSettingsTimeout}, // timeout on settings
	&http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeEnhanceYourCalm}, // server asking to slow down
	&http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeConnect},         // connection issues

	// Same codes for StreamError
	&http2.StreamError{StreamID: 1, Code: http2.ErrCodeProtocol},
	&http2.StreamError{StreamID: 1, Code: http2.ErrCodeInternal},
	&http2.StreamError{StreamID: 1, Code: http2.ErrCodeFlowControl},
	&http2.StreamError{StreamID: 1, Code: http2.ErrCodeSettingsTimeout},
	&http2.StreamError{StreamID: 1, Code: http2.ErrCodeEnhanceYourCalm},
	&http2.StreamError{StreamID: 1, Code: http2.ErrCodeConnect},

	// DNS errors
	&net.DNSError{Err: "timeout", IsTimeout: true, IsTemporary: true},
	&net.DNSError{Err: "temporary failure", IsTimeout: false, IsTemporary: true},

	// Network operation errors
	&net.OpError{Op: "write", Err: &os.SyscallError{Syscall: "write", Err: syscall.ECONNRESET}},
	&net.OpError{Op: "read", Err: &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT}},
	&net.OpError{Op: "write", Err: &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}},
	&net.OpError{Op: "connect", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}},
	&net.OpError{Op: "dial", Err: &net.DNSError{Err: "lookup failed", IsTimeout: true}},
}
