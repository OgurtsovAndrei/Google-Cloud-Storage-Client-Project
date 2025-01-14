package writers

import (
	"awesomeProject/retrier"
	"awesomeProject/utils"
	"context"
	"fmt"
	"io"
	"time"
)

type UnreliableGCSWriter struct {
	gcsClient  *utils.GcsClient
	uploadUrl  string
	resumeOff  int64
	isAborted  bool
	bucket     string
	objectName string
	config     retrier.RetryConfig
}

func NewUnreliableGCSWriter(ctx context.Context, bucket, objectName string) (*UnreliableGCSWriter, error) {
	config := retrier.RetryConfig{
		MaxRetries:      3,
		InitialInterval: 2 * time.Second,
		MaxInterval:     10 * time.Second,
		Multiplier:      2.0,
		MaxJitter:       1 * time.Second,
	}

	var gcsClient *utils.GcsClient
	var uploadUrl string

	err := retrier.RetryWithBackoff(ctx, "initialize_gcs_client", config, func(attempt int) error {
		var err error
		gcsClient, err = utils.NewGcsClient(ctx)
		if err != nil {
			return retrier.NewRetryableError(err, true, "new_gcs_client", attempt)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	err = retrier.RetryWithBackoff(ctx, "create_upload_session", config, func(attempt int) error {
		var err error
		uploadUrl, err = gcsClient.NewUploadSession(ctx, bucket, objectName)
		if err != nil {
			return retrier.NewRetryableError(err, true, "new_upload_session", attempt)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create upload session: %w", err)
	}

	return &UnreliableGCSWriter{
		gcsClient:  gcsClient,
		uploadUrl:  uploadUrl,
		resumeOff:  0,
		isAborted:  false,
		bucket:     bucket,
		objectName: objectName,
		config:     config,
	}, nil
}

func (ugw *UnreliableGCSWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader io.Reader, isLast bool) (int64, error) {
	if ugw.isAborted {
		return 0, retrier.NewRetryableError(
			fmt.Errorf("operation aborted"),
			false,
			"write_at",
			0,
		)
	}
	if chunkBegin != ugw.resumeOff {
		return 0, retrier.NewRetryableError(
			fmt.Errorf("chunk begin %d does not match resume offset %d", chunkBegin, ugw.resumeOff),
			false,
			"write_at",
			0,
		)
	}
	size := chunkEnd - chunkBegin
	var uploadErr error

	err := retrier.RetryWithBackoff(ctx, "upload_part", ugw.config, func(attempt int) error {
		writeStart := time.Now()
		err := ugw.gcsClient.UploadObjectPart(ctx, ugw.uploadUrl, chunkBegin, reader, size, isLast)
		if err != nil {
			uploadErr = err
			ugw.resumeOff = chunkBegin
			return retrier.NewRetryableError(err, true, "upload_object_part", attempt)
		}

		writeDuration := time.Since(writeStart).Seconds()
		uploadSpeed := float64(size) / writeDuration / (1024 * 1024) // MB/s
		fmt.Printf("Uploaded %d bytes at offset %d with speed %.2f MB/s (attempt %d)\n",
			size, chunkBegin, uploadSpeed, attempt+1)

		return nil
	})

	if err != nil {
		return 0, fmt.Errorf("failed to upload part after retries: %w", uploadErr)
	}

	ugw.resumeOff = chunkBegin + size
	return size, nil
}

func (ugw *UnreliableGCSWriter) GetResumeOffset(ctx context.Context) (int64, error) {
	if ugw.isAborted {
		return 0, retrier.NewRetryableError(
			fmt.Errorf("operation aborted"),
			false,
			"get_resume_offset",
			0,
		)
	}

	var offset int64
	var complete bool

	err := retrier.RetryWithBackoff(ctx, "get_resume_offset", ugw.config, func(attempt int) error {
		var err error
		offset, complete, err = ugw.gcsClient.GetResumeOffset(ctx, ugw.uploadUrl)
		if err != nil {
			return retrier.NewRetryableError(err, true, "get_resume_offset", attempt)
		}
		return nil
	})

	if err != nil {
		return 0, err
	}
	if complete {
		return offset, nil
	}
	ugw.resumeOff = offset
	return ugw.resumeOff, nil
}

func (ugw *UnreliableGCSWriter) Abort(ctx context.Context) {
	ugw.isAborted = true

	_ = retrier.RetryWithBackoff(ctx, "cancel_upload", ugw.config, func(attempt int) error {
		if err := ugw.gcsClient.CancelUpload(ctx, ugw.uploadUrl); err != nil {
			return retrier.NewRetryableError(err, true, "cancel_upload", attempt)
		}
		return nil
	})
}
