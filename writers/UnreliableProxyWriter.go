package writers

import (
	"awesomeProject/retrier"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

type UnreliableProxyWriter struct {
	connection     net.Conn
	bucket         string
	objectName     string
	currentOffset  int64
	isAborted      bool
	sequenceNumber uint32
	config         retrier.RetryConfig
	serverAddress  string // Added to support reconnection
}

func NewUnreliableProxyWriter(proxyAddress, bucket, objectName string) (*UnreliableProxyWriter, error) {
	config := retrier.RetryConfig{
		MaxRetries:      3,
		InitialInterval: 1 * time.Second,
		MaxInterval:     5 * time.Second,
		Multiplier:      1.5,
		MaxJitter:       500 * time.Millisecond,
	}

	writer := &UnreliableProxyWriter{
		bucket:        bucket,
		objectName:    objectName,
		serverAddress: proxyAddress,
		config:        config,
	}

	err := retrier.RetryWithBackoff(context.Background(), "connect_to_proxy", config, func(attempt int) error {
		conn, err := net.Dial("tcp", proxyAddress)
		if err != nil {
			return retrier.NewRetryableError(err, true, "dial_tcp", attempt)
		}
		writer.connection = conn
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to GCSProxyServer: %w", err)
	}

	if err := writer.sendInitConnectionRequest(); err != nil {
		err := writer.connection.Close()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("failed to initialize connection: %w", err)
	}

	return writer, nil
}

func (upw *UnreliableProxyWriter) sendInitConnectionRequest() error {
	upw.sequenceNumber++
	header := RequestHeader{
		SequenceNumber: upw.sequenceNumber,
		RequestType:    MessageTypeInitConnection,
	}

	bucketNameBytes := []byte(upw.bucket)
	objectNameBytes := []byte(upw.objectName)

	initReq := InitConnectionRequestHeader{
		BucketNameLength: uint32(len(bucketNameBytes)),
		ObjectNameLength: uint32(len(objectNameBytes)),
	}

	reqSize := binary.Size(initReq) + len(bucketNameBytes) + len(objectNameBytes)
	header.RequestSize = uint32(reqSize)

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("failed to write request header: %w", err)
	}
	if err := binary.Write(buf, binary.BigEndian, &initReq); err != nil {
		return fmt.Errorf("failed to write InitConnectionRequestHeader: %w", err)
	}
	buf.Write(bucketNameBytes)
	buf.Write(objectNameBytes)

	if _, err := upw.connection.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to send init connection request: %w", err)
	}

	return nil
}

func (upw *UnreliableProxyWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader io.Reader, isLast bool) (int64, error) {
	if upw.isAborted {
		return 0, retrier.NewRetryableError(fmt.Errorf("operation aborted"), false, "write_at", 0)
	}

	if chunkBegin != upw.currentOffset {
		return 0, retrier.NewRetryableError(
			fmt.Errorf("chunk begin %d does not match current offset %d", chunkBegin, upw.currentOffset),
			false,
			"write_at",
			0,
		)
	}

	size := chunkEnd - chunkBegin
	if size <= 0 {
		return 0, retrier.NewRetryableError(fmt.Errorf("invalid chunk size"), false, "write_at", 0)
	}

	var bytesWritten int64
	err := retrier.RetryWithBackoff(ctx, "write_chunk", upw.config, func(attempt int) error {
		if err := upw.ensureConnection(ctx); err != nil {
			return retrier.NewRetryableError(err, true, "ensure_connection", attempt)
		}

		upw.sequenceNumber++
		header := RequestHeader{
			SequenceNumber: upw.sequenceNumber,
			RequestType:    MessageTypeUploadPart,
		}

		writeAtReq := WriteAtRequestHeader{
			ChunkBegin: chunkBegin,
			ChunkEnd:   chunkEnd,
			IsLast:     boolToByte(isLast),
		}

		reqSize := binary.Size(writeAtReq) + int(size)
		header.RequestSize = uint32(reqSize)

		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
			return retrier.NewRetryableError(err, false, "write_header", attempt)
		}
		if err := binary.Write(buf, binary.BigEndian, &writeAtReq); err != nil {
			return retrier.NewRetryableError(err, false, "write_request", attempt)
		}

		startTime := time.Now()

		// Write headers
		if _, err := upw.connection.Write(buf.Bytes()); err != nil {
			return retrier.NewRetryableError(err, true, "write_metadata", attempt)
		}

		// Write data
		written, err := io.CopyN(upw.connection, reader, size)
		if err != nil {
			upw.currentOffset = chunkBegin
			return retrier.NewRetryableError(err, true, "write_data", attempt)
		}

		bytesWritten = written
		elapsedTime := time.Since(startTime)
		uploadSpeed := float64(written) / elapsedTime.Seconds()
		fmt.Printf("Sent chunk to TCP [%d - %d] (%d bytes) in %.2f seconds (%.2f MB/s) (attempt %d)\n",
			chunkBegin, chunkEnd, written, elapsedTime.Seconds(), uploadSpeed/(1024*1024), attempt+1)

		return nil
	})

	if err != nil {
		return bytesWritten, err
	}

	upw.currentOffset = chunkEnd
	return bytesWritten, nil
}

func (upw *UnreliableProxyWriter) GetResumeOffset(ctx context.Context) (int64, error) {
	if upw.isAborted {
		return 0, retrier.NewRetryableError(fmt.Errorf("operation aborted"), false, "get_resume_offset", 0)
	}

	var resumeOffset int64
	err := retrier.RetryWithBackoff(ctx, "get_resume_offset", upw.config, func(attempt int) error {
		if err := upw.ensureConnection(ctx); err != nil {
			return retrier.NewRetryableError(err, true, "ensure_connection", attempt)
		}

		upw.sequenceNumber++
		header := RequestHeader{
			SequenceNumber: upw.sequenceNumber,
			RequestType:    MessageTypeGetResumeOffset,
			RequestSize:    0,
		}

		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
			return retrier.NewRetryableError(err, false, "write_header", attempt)
		}

		if _, err := upw.connection.Write(buf.Bytes()); err != nil {
			return retrier.NewRetryableError(err, true, "write_request", attempt)
		}

		if err := binary.Read(upw.connection, binary.BigEndian, &resumeOffset); err != nil {
			return retrier.NewRetryableError(err, true, "read_response", attempt)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	upw.currentOffset = resumeOffset
	return resumeOffset, nil
}

func (upw *UnreliableProxyWriter) Abort(ctx context.Context) {
	if upw.isAborted {
		return
	}
	upw.isAborted = true

	_ = retrier.RetryWithBackoff(ctx, "abort_upload", upw.config, func(attempt int) error {
		if err := upw.ensureConnection(ctx); err != nil {
			return retrier.NewRetryableError(err, true, "ensure_connection", attempt)
		}

		upw.sequenceNumber++
		header := RequestHeader{
			SequenceNumber: upw.sequenceNumber,
			RequestType:    MessageTypeAbort,
			RequestSize:    0,
		}

		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
			return retrier.NewRetryableError(err, false, "write_header", attempt)
		}

		if _, err := upw.connection.Write(buf.Bytes()); err != nil {
			return retrier.NewRetryableError(err, true, "write_request", attempt)
		}

		return nil
	})

	if upw.connection != nil {
		err := upw.connection.Close()
		if err != nil {
			return
		}
		upw.connection = nil
	}
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

func (upw *UnreliableProxyWriter) ensureConnection(ctx context.Context) error {
	if upw.connection != nil {
		// Try a quick connection check
		err := upw.connection.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		if err == nil {
			return nil
		}
	}

	// Connection is dead or nil, attempt to reconnect
	return retrier.RetryWithBackoff(ctx, "reconnect_to_proxy", upw.config, func(attempt int) error {
		conn, err := net.Dial("tcp", upw.serverAddress)
		if err != nil {
			return retrier.NewRetryableError(err, true, "reconnect_dial", attempt)
		}
		upw.connection = conn

		// Re-initialize the connection
		if err := upw.sendInitConnectionRequest(); err != nil {
			err := conn.Close()
			if err != nil {
				return err
			}
			return retrier.NewRetryableError(err, true, "reinit_connection", attempt)
		}
		return nil
	})
}
