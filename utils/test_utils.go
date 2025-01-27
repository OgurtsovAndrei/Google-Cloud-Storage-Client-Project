package utils

import (
	"cloud.google.com/go/storage"
	"context"
	"github.com/schollz/progressbar/v3"
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

type ErrorInjector interface {
	ShouldInjectError() bool
	GetError() error
}

type NetworkFaultInjector struct {
	FaultRatio float64
	Errors     []error
	rnd        *rand.Rand
}

func NewNetworkFaultInjector(faultRatio float64, errors []error) *NetworkFaultInjector {
	return &NetworkFaultInjector{
		FaultRatio: faultRatio,
		Errors:     errors,
		rnd:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (n *NetworkFaultInjector) ShouldInjectError() bool {
	if n.rnd == nil {
		n.rnd = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return n.rnd.Float64() < n.FaultRatio
}

func (n *NetworkFaultInjector) GetError() error {
	println("selecting error")
	return n.Errors[n.rnd.Intn(len(n.Errors))]
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

var (
	TemporaryNetworkErrors = []error{
		&net.OpError{Op: "write", Err: syscall.ECONNRESET},
		&net.OpError{Op: "read", Err: syscall.ECONNRESET},
		&net.OpError{Op: "write", Err: &os.SyscallError{Syscall: "write", Err: syscall.ETIMEDOUT}},
		&net.OpError{Op: "read", Err: &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT}},
		&net.OpError{Op: "connect", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}},
		&net.OpError{Op: "write", Err: &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}},
	}
)
