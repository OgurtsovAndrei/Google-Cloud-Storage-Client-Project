package writers

import (
	"awesomeProject/retrier"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

type mockUnreliableWriter struct {
	mu              sync.Mutex
	writeAttempts   []time.Time
	offsetAttempts  []time.Time
	abortAttempts   []time.Time
	failureCount    int
	simulatePartial bool
	writtenBytes    int64
}

func (m *mockUnreliableWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader io.Reader, isLast bool) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.writeAttempts = append(m.writeAttempts, time.Now())
	attemptNum := len(m.writeAttempts)

	if attemptNum <= m.failureCount {
		if attemptNum%2 == 0 {
			return 0, errors.New("connection reset by peer")
		} else {
			return 0, fmt.Errorf("server error: status code 503")
		}
	}

	requestedBytes := chunkEnd - chunkBegin
	if m.simulatePartial && attemptNum == m.failureCount+1 {
		bytesToWrite := requestedBytes / 2
		m.writtenBytes += bytesToWrite
		return bytesToWrite, nil
	}

	m.writtenBytes += requestedBytes
	return requestedBytes, nil
}

func (m *mockUnreliableWriter) GetResumeOffset(ctx context.Context) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.offsetAttempts = append(m.offsetAttempts, time.Now())
	attemptNum := len(m.offsetAttempts)

	if attemptNum <= m.failureCount {
		return 0, errors.New("connection reset by peer")
	}

	return m.writtenBytes, nil
}

func (m *mockUnreliableWriter) Abort(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.abortAttempts = append(m.abortAttempts, time.Now())
}

func validateBackoffIntervals(t *testing.T, attempts []time.Time, config retrier.RetryConfig) {
	if len(attempts) <= 1 {
		return
	}

	expectedMin := config.InitialInterval
	for i := 1; i < len(attempts); i++ {
		interval := attempts[i].Sub(attempts[i-1])

		maxWithJitter := expectedMin + config.MaxJitter
		minWithJitter := expectedMin - config.MaxJitter

		if interval < minWithJitter || interval > maxWithJitter {
			t.Errorf("Attempt %d: interval %v not within expected range [%v, %v]",
				i, interval, minWithJitter, maxWithJitter)
		}

		expectedMin = time.Duration(float64(expectedMin) * config.Multiplier)
		if expectedMin > config.MaxInterval {
			expectedMin = config.MaxInterval
		}
	}
}

func TestReliableWriter_RetryBehavior(t *testing.T) {
	tests := []struct {
		name            string
		failureCount    int
		simulatePartial bool
		expectError     bool
		writeSize       int
	}{
		{
			name:         "retries_network_errors",
			failureCount: 3,
			writeSize:    1024 * 1024,
			expectError:  false,
		},
		{
			name:            "handles_partial_write",
			failureCount:    2,
			simulatePartial: true,
			writeSize:       2 * 1024 * 1024,
			expectError:     false,
		},
		{
			name:         "fails_after_max_retries",
			failureCount: 6,
			writeSize:    1024 * 1024,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			mock := &mockUnreliableWriter{
				failureCount:    tt.failureCount,
				simulatePartial: tt.simulatePartial,
			}

			writer := NewReliableWriterImpl(ctx, mock, ReliableWriterConfig{
				MaxCacheSize: 8 * 1024 * 1024,
				MinChunkSize: 1024 * 1024,
				MaxChunkSize: 4 * 1024 * 1024,
			})

			testData := make([]byte, tt.writeSize)
			err := writer.WriteAt(ctx, testData, 0)
			if err != nil {
				t.Fatalf("WriteAt failed: %v", err)
			}

			err = writer.Complete(ctx)

			if tt.expectError {
				if err == nil {
					t.Fatal("Expected error but got nil")
				}

				var retryErr *retrier.RetryableError
				if errors.As(err, &retryErr) {
					if retryErr.Operation != "write_chunk" && retryErr.Operation != "validate_completion" {
						t.Errorf("Unexpected operation in error: %s", retryErr.Operation)
					}
				} else {
					t.Errorf("Expected RetryableError, got %T", err)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if mock.writtenBytes != int64(tt.writeSize) {
					t.Errorf("Expected %d bytes written, got %d", tt.writeSize, mock.writtenBytes)
				}
			}

			validateBackoffIntervals(t, mock.writeAttempts, writer.retryConfig)
			validateBackoffIntervals(t, mock.offsetAttempts, writer.retryConfig)
		})
	}
}

func TestReliableWriter_ConcurrentRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mock := &mockUnreliableWriter{failureCount: 2}
	writer := NewReliableWriterImpl(ctx, mock, ReliableWriterConfig{
		MaxCacheSize: 8 * 1024 * 1024,
		MinChunkSize: 1024 * 1024,
		MaxChunkSize: 4 * 1024 * 1024,
	})

	var wg sync.WaitGroup
	writeSize := 1024 * 1024
	concurrentWrites := 3

	for i := 0; i < concurrentWrites; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			data := make([]byte, writeSize)
			err := writer.WriteAt(ctx, data, int64(offset*writeSize))
			if err != nil {
				t.Errorf("Concurrent write failed: %v", err)
			}
		}(i)
	}

	wg.Wait()
	err := writer.Complete(ctx)
	if err != nil {
		t.Fatalf("Complete failed: %v", err)
	}

	expectedBytes := int64(writeSize * concurrentWrites)
	if mock.writtenBytes != expectedBytes {
		t.Errorf("Expected %d total bytes written, got %d", expectedBytes, mock.writtenBytes)
	}
}

func TestReliableWriter_AbortRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mock := &mockUnreliableWriter{}
	writer := NewReliableWriterImpl(ctx, mock, ReliableWriterConfig{
		MaxCacheSize: 8 * 1024 * 1024,
		MinChunkSize: 1024 * 1024,
		MaxChunkSize: 4 * 1024 * 1024,
	})

	writer.Abort(ctx)

	if len(mock.abortAttempts) != 1 {
		t.Errorf("Expected 1 abort attempt, got %d", len(mock.abortAttempts))
	}
}
