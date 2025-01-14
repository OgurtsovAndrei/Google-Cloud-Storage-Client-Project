package retrier

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
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

		if retryErr, ok := err.(*RetryableError); ok && !retryErr.Retriable {
			return err
		}

		if !IsRetryableError(err) {
			return &RetryableError{
				Err:       err,
				Retriable: false,
				Operation: op,
				Attempt:   attempt,
			}
		}

		if attempt == config.MaxRetries {
			return fmt.Errorf("operation %s failed after %d attempts: %v", op, attempt+1, err)
		}

		// Add jitter to prevent thundering herd
		jitter := time.Duration(float64(config.MaxJitter) * (0.5 + rand.Float64()))
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
			interval = time.Duration(float64(interval) * config.Multiplier)
			if interval > config.MaxInterval {
				interval = config.MaxInterval
			}
		}
	}
	return nil
}

func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	var retryErr *RetryableError
	if errors.As(err, &retryErr) {
		return retryErr.Retriable
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Temporary() || netErr.Timeout()
	}

	var httpRespErr *http.Response
	if errors.As(err, &httpRespErr) {
		code := httpRespErr.StatusCode
		return code >= 500 || code == 429 || code == 408
	}

	errStr := strings.ToLower(err.Error())
	retryableStrings := []string{
		"connection reset",
		"broken pipe",
		"connection refused",
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
