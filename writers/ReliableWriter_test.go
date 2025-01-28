package writers

import (
	"awesomeProject/retrier"
	"awesomeProject/utils"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/net/http2"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	result := m.Run()

	if testing.CoverMode() != "" {
		coverage := testing.Coverage()
		fmt.Printf("\nTest coverage: %.2f%%\n", coverage*100)
	}

	os.Exit(result)
}

func TestRealGCSUpload(t *testing.T) {
	bucket := "another-eu-1-reg-bucket-finland-es"
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	fileSize := 1 << 30 // 1GB
	fileName := fmt.Sprintf("test_upload_%d.dat", time.Now().Unix())

	fmt.Printf("\nGenerating %d bytes of test data...\n", fileSize)
	testData := utils.GenerateTestPattern(fileSize)
	expectedHash := sha256.Sum256(testData)

	injector := utils.NewNetworkFaultInjector(0.001, utils.TemporaryNetworkErrors)

	writer, err := NewUnreliableGCSWriter(ctx, bucket, fileName, injector)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	reliableWriter := NewReliableWriterImpl(ctx, writer, ReliableWriterConfig{
		MaxCacheSize: 16 << 20, // 16MB
		MinChunkSize: 1 << 20,  // 1MB
		MaxChunkSize: 8 << 20,  // 8MB
	})

	var writtenData []byte
	var writeMutex sync.Mutex
	writer.writeHook = func(data []byte, offset int64) {
		writeMutex.Lock()
		defer writeMutex.Unlock()
		if offset+int64(len(data)) > int64(len(writtenData)) {
			newData := make([]byte, offset+int64(len(data)))
			copy(newData, writtenData)
			writtenData = newData
		}
		copy(writtenData[offset:], data)
	}

	bar := progressbar.NewOptions64(
		int64(len(testData)),
		progressbar.OptionSetDescription("Uploading"),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(30),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionSetRenderBlankState(true),
	)

	// write with varying chunk sizes
	chunkSizes := []int{1 << 20, 2 << 20, 4 << 20, 8 << 20}
	for offset := 0; offset < len(testData); {
		chunkSize := chunkSizes[offset/len(chunkSizes)%len(chunkSizes)]
		if offset+chunkSize > len(testData) {
			chunkSize = len(testData) - offset
		}

		err := reliableWriter.WriteAt(ctx, testData[offset:offset+chunkSize], int64(offset))
		if err != nil {
			t.Fatalf("Write failed at offset %d: %v", offset, err)
		}
		_ = bar.Add(chunkSize)
		offset += chunkSize
	}

	if err := reliableWriter.Complete(ctx); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	fmt.Printf("\nVerifying data integrity during upload...\n")
	utils.VerifyChunkPositions(t, writtenData)

	fmt.Printf("\nDownloading file from GCS for final verification...\n")
	downloadedData, err := utils.DownloadFromGCS(ctx, bucket, fileName)
	if err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	downloadedHash := sha256.Sum256(downloadedData)
	if hex.EncodeToString(downloadedHash[:]) != hex.EncodeToString(expectedHash[:]) {
		t.Error("Downloaded data hash mismatch")
		utils.VerifyChunkPositions(t, downloadedData)
	} else {
		fmt.Printf("\nData integrity verification successful! ✓\n")
		fmt.Printf("File size: %d bytes\n", len(downloadedData))
		fmt.Printf("SHA256: %x\n", downloadedHash)
	}
}

func TestIsRetryableError(t *testing.T) {
	testCases := []struct {
		name        string
		err         error
		shouldRetry bool
	}{
		// TLS errors
		{name: "tls_bad_record_mac", err: tls.AlertError(20), shouldRetry: true},
		{name: "tls_unexpected_message", err: tls.AlertError(10), shouldRetry: true},
		{name: "tls_handshake_failure", err: tls.AlertError(40), shouldRetry: true},
		{name: "tls_internal_error", err: tls.AlertError(80), shouldRetry: true},
		{name: "tls_protocol_version", err: tls.AlertError(70), shouldRetry: true},
		{name: "tls_insufficient_security", err: tls.AlertError(71), shouldRetry: true},
		{name: "tls_decode_error", err: tls.AlertError(50), shouldRetry: true},
		{name: "tls_record_overflow", err: tls.AlertError(22), shouldRetry: true},
		{name: "tls_certificate_expired", err: tls.AlertError(45), shouldRetry: false},
		{name: "tls_unknown_ca", err: tls.AlertError(48), shouldRetry: false},

		// HTTP/2 errors
		{name: "h2_goaway_protocol", err: &http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeProtocol}, shouldRetry: true},
		{name: "h2_goaway_internal", err: &http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeInternal}, shouldRetry: true},
		{name: "h2_goaway_flow_control", err: &http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeFlowControl}, shouldRetry: true},
		{name: "h2_goaway_settings_timeout", err: &http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeSettingsTimeout}, shouldRetry: true},
		{name: "h2_goaway_enhance_calm", err: &http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeEnhanceYourCalm}, shouldRetry: true},
		{name: "h2_goaway_connect", err: &http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeConnect}, shouldRetry: true},
		{name: "h2_goaway_inadequate_security", err: &http2.GoAwayError{LastStreamID: 1, ErrCode: http2.ErrCodeInadequateSecurity}, shouldRetry: false},

		{name: "h2_stream_protocol", err: &http2.StreamError{StreamID: 1, Code: http2.ErrCodeProtocol}, shouldRetry: true},
		{name: "h2_stream_internal", err: &http2.StreamError{StreamID: 1, Code: http2.ErrCodeInternal}, shouldRetry: true},
		{name: "h2_stream_flow_control", err: &http2.StreamError{StreamID: 1, Code: http2.ErrCodeFlowControl}, shouldRetry: true},
		{name: "h2_stream_settings_timeout", err: &http2.StreamError{StreamID: 1, Code: http2.ErrCodeSettingsTimeout}, shouldRetry: true},
		{name: "h2_stream_enhance_calm", err: &http2.StreamError{StreamID: 1, Code: http2.ErrCodeEnhanceYourCalm}, shouldRetry: true},
		{name: "h2_stream_connect", err: &http2.StreamError{StreamID: 1, Code: http2.ErrCodeConnect}, shouldRetry: true},
		{name: "h2_stream_cancel", err: &http2.StreamError{StreamID: 1, Code: http2.ErrCodeCancel}, shouldRetry: false},

		// Keep existing test cases
		{name: "retryable_gcs_429", err: &retrier.GCSError{Code: 429, Message: "Too Many Requests"}, shouldRetry: true},
		{name: "retryable_gcs_500", err: &retrier.GCSError{Code: 500, Message: "Internal Server Error"}, shouldRetry: true},
		{name: "retryable_gcs_502", err: &retrier.GCSError{Code: 502, Message: "Bad Gateway"}, shouldRetry: true},
		{name: "retryable_gcs_503", err: &retrier.GCSError{Code: 503, Message: "Service Unavailable"}, shouldRetry: true},
		{name: "retryable_gcs_504", err: &retrier.GCSError{Code: 504, Message: "Gateway Timeout"}, shouldRetry: true},
		{name: "non_retryable_gcs_400", err: &retrier.GCSError{Code: 400, Message: "Bad Request"}, shouldRetry: false},
		{name: "non_retryable_gcs_401", err: &retrier.GCSError{Code: 401, Message: "Unauthorized"}, shouldRetry: false},
		{name: "non_retryable_gcs_403", err: &retrier.GCSError{Code: 403, Message: "Forbidden"}, shouldRetry: false},
		{name: "non_retryable_gcs_404", err: &retrier.GCSError{Code: 404, Message: "Not Found"}, shouldRetry: false},
		{name: "non_retryable_gcs_409", err: &retrier.GCSError{Code: 409, Message: "Conflict"}, shouldRetry: false},

		// Network errors
		{name: "dns_timeout", err: &net.DNSError{Err: "timeout", IsTimeout: true, IsTemporary: true}, shouldRetry: true},
		{name: "dns_temp", err: &net.DNSError{Err: "temporary failure", IsTimeout: false, IsTemporary: true}, shouldRetry: true},
		{name: "dns_not_found", err: &net.DNSError{Err: "no such host", IsTimeout: false, IsTemporary: false}, shouldRetry: false},

		{name: "op_write_reset", err: &net.OpError{Op: "write", Err: &os.SyscallError{Syscall: "write", Err: syscall.ECONNRESET}}, shouldRetry: true},
		{name: "op_read_timeout", err: &net.OpError{Op: "read", Err: &os.SyscallError{Syscall: "read", Err: syscall.ETIMEDOUT}}, shouldRetry: true},
		{name: "op_write_pipe", err: &net.OpError{Op: "write", Err: &os.SyscallError{Syscall: "write", Err: syscall.EPIPE}}, shouldRetry: true},
		{name: "op_connect_refused", err: &net.OpError{Op: "connect", Err: &os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}}, shouldRetry: true},

		// String matching
		{name: "str_conn_reset", err: fmt.Errorf("connection reset by peer"), shouldRetry: true},
		{name: "str_broken_pipe", err: fmt.Errorf("broken pipe"), shouldRetry: true},
		{name: "str_conn_refused", err: fmt.Errorf("connection refused"), shouldRetry: true},
		{name: "str_too_many", err: fmt.Errorf("too many requests"), shouldRetry: true},
		{name: "str_unavailable", err: fmt.Errorf("service unavailable"), shouldRetry: true},
		{name: "str_gateway", err: fmt.Errorf("gateway timeout"), shouldRetry: true},
		{name: "str_timeout", err: fmt.Errorf("request timeout"), shouldRetry: true},
		{name: "str_random", err: fmt.Errorf("some random error"), shouldRetry: false},
		{name: "nil_error", err: nil, shouldRetry: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := retrier.IsRetryableError(tc.err)
			if result != tc.shouldRetry {
				t.Errorf("IsRetryableError(%v) = %v, want %v\nError type: %T",
					tc.err, result, tc.shouldRetry, tc.err)
			}
		})
	}
}

func TestUnreliableGCSWriterOffsetMismatch(t *testing.T) {
	ctx := context.Background()
	bucket := "another-eu-1-reg-bucket-finland-es"
	fileName := fmt.Sprintf("test_upload_%d.dat", time.Now().Unix())

	writer, err := NewUnreliableGCSWriter(ctx, bucket, fileName, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	writer.resumeOff = 1000
	data := []byte("test data")
	reader := bytes.NewReader(data)

	written, err := writer.WriteAt(ctx, 0, int64(len(data)), reader, false)

	if written != 0 {
		t.Errorf("expected 0 but got %d", written)
	}

	var retryErr *retrier.RetryableError
	if !errors.As(err, &retryErr) {
		t.Fatal("expected RetryableError")
	}

	if !retryErr.Retriable {
		t.Error("expected offset mismatch error to be retryable")
	}

	var gcsErr *retrier.GCSError
	if !errors.As(retryErr.Err, &gcsErr) {
		t.Fatal("expected GCSError inside RetryableError")
	}

	if gcsErr.Code != 400 {
		t.Errorf("expected error code 400, got %d", gcsErr.Code)
	}
}

func TestUnreliableGCSWriterAborted(t *testing.T) {
	ctx := context.Background()
	bucket := "another-eu-1-reg-bucket-finland-es"
	fileName := fmt.Sprintf("test_upload_%d.dat", time.Now().Unix())

	writer, err := NewUnreliableGCSWriter(ctx, bucket, fileName, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	writer.isAborted = true
	data := []byte("test data")
	reader := bytes.NewReader(data)

	written, err := writer.WriteAt(ctx, 0, int64(len(data)), reader, false)

	if written != 0 {
		t.Errorf("expected 0 but got %d", written)
	}

	var retryErr *retrier.RetryableError
	if !errors.As(err, &retryErr) {
		t.Fatal("expected RetryableError")
	}

	var gcsErr *retrier.GCSError
	if !errors.As(retryErr.Err, &gcsErr) {
		t.Fatal("expected GCSError")
	}

	if gcsErr.Code != 499 {
		t.Errorf("expected error code 499, got %d", gcsErr.Code)
	}
}
