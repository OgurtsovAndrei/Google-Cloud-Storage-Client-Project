package retrier

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"golang.org/x/net/http2"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"
)

type RetryConfig struct {
	MaxRetries      int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	MaxJitter       time.Duration
}

func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxRetries:      5,
		InitialInterval: 1 * time.Second,
		MaxInterval:     30 * time.Second,
		Multiplier:      2.0,
		MaxJitter:       500 * time.Millisecond,
	}
}

type RetryableError struct {
	Err       error
	Retriable bool
	Operation string
	Attempt   int
}

type HTTPResponseError struct {
	Response *http.Response
}

func (e *HTTPResponseError) Error() string {
	return fmt.Sprintf("HTTP error: status code %d", e.Response.StatusCode)
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("%s failed (attempt %d): %v (retriable: %v)",
		e.Operation, e.Attempt, e.Err, e.Retriable)
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

func RetryWithBackoff(ctx context.Context, op string, config RetryConfig, fn func(attempt int) error) error {
	interval := config.InitialInterval

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		err := fn(attempt)
		if err == nil {
			return nil
		}

		var retryErr *RetryableError
		if errors.As(err, &retryErr) {
			if !retryErr.Retriable {
				return err
			}
		} else {
			isRetryable := IsRetryableError(err)
			err = NewRetryableError(err, isRetryable, op, attempt)
			if !isRetryable {
				return err
			}
		}

		if attempt == config.MaxRetries {
			return fmt.Errorf("operation %s failed after %d attempts: %v", op, attempt+1, err)
		}

		jitter := time.Duration(float64(config.MaxJitter) * rand.Float64())
		backoff := interval + jitter

		select {
		case <-ctx.Done():
			return &RetryableError{
				Err:       ctx.Err(),
				Retriable: false,
				Operation: op,
				Attempt:   attempt,
			}
		case <-time.After(backoff):
		}

		// Calculate next interval after waiting
		interval = time.Duration(float64(interval) * config.Multiplier)
		if interval > config.MaxInterval {
			interval = config.MaxInterval
		}
	}
	return nil
}

type GCSError struct {
	Code    int
	Message string
	Inner   error
}

func (e *GCSError) Error() string {
	return fmt.Sprintf("GCS error: %s (code: %d)", e.Message, e.Code)
}

func (e *GCSError) Unwrap() error {
	return e.Inner
}

func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	var retryErr *RetryableError
	if errors.As(err, &retryErr) {
		return retryErr.Retriable
	}

	var gcsErr *GCSError
	if errors.As(err, &gcsErr) {
		switch gcsErr.Code {
		case 408, 429, 500, 502, 503, 504:
			return true
		default:
			return false
		}
	}

	// TLS alerts
	var alertErr tls.AlertError
	if errors.As(err, &alertErr) {
		switch alertErr {
		case 10, // alertUnexpectedMessage
			20, // alertBadRecordMAC
			22, // alertRecordOverflow
			40, // alertHandshakeFailure
			50, // alertDecodeError
			70, // alertProtocolVersion
			71, // alertInsufficientSecurity
			80: // alertInternalError
			return true
		default:
			return false
		}
	}

	// HTTP/2 errors
	var goAwayErr *http2.GoAwayError
	if errors.As(err, &goAwayErr) {
		switch goAwayErr.ErrCode {
		case http2.ErrCodeProtocol,
			http2.ErrCodeInternal,
			http2.ErrCodeFlowControl,
			http2.ErrCodeSettingsTimeout,
			http2.ErrCodeEnhanceYourCalm,
			http2.ErrCodeConnect:
			return true
		default:
			return false
		}
	}

	var streamErr *http2.StreamError
	if errors.As(err, &streamErr) {
		switch streamErr.Code {
		case http2.ErrCodeProtocol,
			http2.ErrCodeInternal,
			http2.ErrCodeFlowControl,
			http2.ErrCodeSettingsTimeout,
			http2.ErrCodeEnhanceYourCalm,
			http2.ErrCodeConnect:
			return true
		default:
			return false
		}
	}

	// DNS errors
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTemporary || dnsErr.IsTimeout
	}

	// Network errors
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr.Timeout() || opErr.Temporary() {
			return true
		}
		if opErr.Err != nil {
			return IsRetryableError(opErr.Err)
		}
	}

	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) {
		switch sysErr.Err {
		case syscall.ECONNRESET,
			syscall.ETIMEDOUT,
			syscall.EPIPE,
			syscall.ECONNREFUSED:
			return true
		default:
			return false
		}
	}

	errStr := strings.ToLower(err.Error())
	retryableStrings := []string{
		"connection reset",
		"broken pipe",
		"connection refused",
		"i/o timeout",
		"temporary",
		"deadline exceeded",
		"too many requests",
		"service unavailable",
		"gateway timeout",
		"request timeout",
	}

	for _, s := range retryableStrings {
		if strings.Contains(errStr, s) {
			return true
		}
	}

	return false
}

func NewRetryableError(err error, retriable bool, operation string, attempt int) *RetryableError {
	return &RetryableError{
		Err:       err,
		Retriable: retriable,
		Operation: operation,
		Attempt:   attempt,
	}
}
