package utils

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// General Tags
const (
	TagRetryable       = "retryable"        // Error can be retried.
	TagContextCanceled = "context-canceled" // Context was canceled.
	TagOutOfOrder      = "out-of-order"     // Data or request is out of order.
	TagIllegalArgument = "illegal-argument" // Invalid arguments or input.
	TagNetwork         = "network"          // Network-related error.
	TagConnectionDown  = "connection-down"  // Connection is down.
	TagTimeout         = "timeout"          // Operation timed out.
	TagNotFound        = "not-found"        // Resource not found.
	TagInternal        = "internal-error"   // Internal server or system error.
)

// Writers Module (UnreliableProxyWriter) Error Codes
const (
	ErrCodeInitSession   = "UPL001" // Failed to initialize upload session.
	ErrCodeSendMessage   = "UPL002" // Failed to send a message.
	ErrCodeWaitResponse  = "UPL003" // Failed to wait for or retrieve a response.
	ErrCodeWriteAtFailed = "UPL004" // Server responded with a failure during WriteAt.
	ErrCodeGetResume     = "UPL005" // Failed to retrieve the resume offset.
	ErrCodeParseOffset   = "UPL006" // Parsing offset data failed.
)

// Proxy Client Connection Group Error Codes
const (
	ErrCodeResponseChannelNotFound = "CONN001" // Response channel not found.
	ErrCodeHandleConnectionFailed  = "CONN002" // Connection failed to establish or maintain.
)

const (
	ContextCancelled        = "CTX001" // Context canceled
	ContextDeadlineExceeded = "CTX002" // Context deadline exceeded
)

// Proxy Server (GcsProxyServer) Error Codes
const (
	ErrCodeUnknownRequestType = "SRV001" // Unknown request type received.
	ErrCodeOutOfOrderWrite    = "SRV002" // Out-of-order write request received.
	ErrCodeInitUploadSession  = "SRV003" // Failed to initialize a new upload session.
	ErrCodeAbortFailed        = "SRV004" // Failed to abort the session.
	ErrCodeUploadChunkFailed  = "SRV005" // Failed to upload a chunk to GCS.
	ErrCodeNotFound           = "SRV006" // Connection upload session not found
)

const (
	ErrCastFailed        = "CastFailed"
	InvalidArgumentError = "InvalidArgumentError"
)

// Error represents a custom error with tags
type Error struct {
	Code  string   `json:"code"`
	Msg   string   `json:"msg"`
	Cause error    `json:"-"`
	Tags  []string `json:"tags,omitempty"` // Tags for additional metadata
}

// Error implements the error interface
func (e *Error) Error() string {
	// Format the base error message
	base := fmt.Sprintf("Code: %s, Message: %s", e.Code, e.Msg)

	// Include Cause if available
	if e.Cause != nil {
		base += fmt.Sprintf(", Cause: %s", e.Cause.Error())
	}

	// Include Tags if available
	if len(e.Tags) > 0 {
		base += fmt.Sprintf(", Tags: [%s]", e.formatTags())
	}

	return base
}

// Helper method to format tags
func (e *Error) formatTags() string {
	return fmt.Sprintf("%s", stringJoin(e.Tags, ", "))
}

// Utility function to join a slice of strings
func stringJoin(slice []string, sep string) string {
	result := ""
	for i, s := range slice {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}

// Unwrap allows unwrapping the Cause error
func (e *Error) Unwrap() error {
	return e.Cause
}

// ToJSON converts the Error to a JSON string
func (e *Error) ToJSON() (string, error) {
	type ErrorJSON struct {
		Code  string   `json:"code"`
		Msg   string   `json:"msg"`
		Cause string   `json:"cause,omitempty"` // Serialize Cause as string
		Tags  []string `json:"tags,omitempty"`  // Include tags
	}

	errJSON := ErrorJSON{
		Code: e.Code,
		Msg:  e.Msg,
		Tags: e.Tags,
	}

	if e.Cause != nil {
		errJSON.Cause = e.Cause.Error()
	}

	data, err := json.Marshal(errJSON)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// FromJSON reconstructs an Error struct from a JSON string
func FromJSON(data string) (*Error, error) {
	type ErrorJSON struct {
		Code  string   `json:"code"`
		Msg   string   `json:"msg"`
		Cause string   `json:"cause,omitempty"` // Deserialize Cause as string
		Tags  []string `json:"tags,omitempty"`  // Deserialize tags
	}

	var errJSON ErrorJSON
	if err := json.Unmarshal([]byte(data), &errJSON); err != nil {
		return nil, err
	}

	var cause error
	if errJSON.Cause != "" {
		cause = errors.New(errJSON.Cause) // Wrap the Cause string into a basic error
	}

	return &Error{
		Code:  errJSON.Code,
		Msg:   errJSON.Msg,
		Cause: cause,
		Tags:  errJSON.Tags,
	}, nil
}

// AddTag adds a tag to the error if it doesn't already exist
func (e *Error) AddTag(tag string) {
	for _, t := range e.Tags {
		if t == tag {
			return // Tag already exists
		}
	}
	e.Tags = append(e.Tags, tag)
}

// RemoveTag removes a tag from the error if it exists
func (e *Error) RemoveTag(tag string) {
	for i, t := range e.Tags {
		if t == tag {
			e.Tags = append(e.Tags[:i], e.Tags[i+1:]...)
			return
		}
	}
}

// HasTag checks if the error contains a specific tag
func (e *Error) HasTag(tag string) bool {
	for _, t := range e.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// CastContextError casts a context error (ctx.Err()) into a utils.Error with the ContextCanceled tag.
// If ctx.Err() is nil or not a context error, it returns nil.
func CastContextError(err error) *Error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return &Error{
			Code:  ContextCancelled,
			Msg:   "Context canceled",
			Cause: err,
			Tags:  []string{TagContextCanceled},
		}
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{
			Code:  ContextDeadlineExceeded,
			Msg:   "Context deadline exceeded",
			Cause: err,
			Tags:  []string{TagTimeout},
		}
	}

	panic("should be called only on 'ctx.Err()'")
}
