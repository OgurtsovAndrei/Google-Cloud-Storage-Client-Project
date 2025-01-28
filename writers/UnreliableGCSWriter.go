package writers

import (
	"awesomeProject/retrier"
	"awesomeProject/utils"
	"context"
	"errors"
	"fmt"
	"io"
)

type UnreliableGCSWriter struct {
	gcsClient  *utils.GcsClient
	uploadUrl  string
	resumeOff  int64
	isAborted  bool
	bucket     string
	objectName string
	//for testing
	writeHook func(data []byte, offset int64)
}

func NewUnreliableGCSWriter(ctx context.Context, bucket, objectName string, injector *utils.NetworkFaultInjector) (*UnreliableGCSWriter, error) {
	gcsClient, err := utils.NewGcsClient(ctx, injector)
	if err != nil {
		return nil, err
	}
	uploadUrl, err := gcsClient.NewUploadSession(ctx, bucket, objectName)
	if err != nil {
		return nil, err
	}
	return &UnreliableGCSWriter{
		gcsClient:  gcsClient,
		uploadUrl:  uploadUrl,
		resumeOff:  0,
		isAborted:  false,
		bucket:     bucket,
		objectName: objectName,
	}, nil
}

func (ugw *UnreliableGCSWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader io.Reader, isLast bool) (int64, error) {
	if ugw.isAborted {
		return 0, retrier.NewRetryableError(
			&retrier.GCSError{Code: 499, Message: "operation aborted"},
			false,
			"write_aborted",
			0,
		)
	}

	if chunkBegin != ugw.resumeOff {
		offsetErr := &retrier.GCSError{
			Code:    400,
			Message: fmt.Sprintf("invalid offset: expected %d, got %d", ugw.resumeOff, chunkBegin),
		}
		// Offset mismatches are retryable
		return 0, retrier.NewRetryableError(offsetErr, true, "check_offset", 0)
	}

	const MinUploadChunkSize = 256 * 1024
	size := chunkEnd - chunkBegin

	if !isLast {
		// align the offset to nearest MinUploadChunkSize boundary
		alignedBegin := (chunkBegin + MinUploadChunkSize - 1) / MinUploadChunkSize * MinUploadChunkSize

		remainingSize := chunkEnd - alignedBegin
		alignedSize := (remainingSize / MinUploadChunkSize) * MinUploadChunkSize
		chunkEnd = alignedBegin + alignedSize
		size = chunkEnd - chunkBegin
	}

	_, err := ugw.gcsClient.UploadObjectPart(ctx, ugw.uploadUrl, chunkBegin, reader, size, isLast)
	if err != nil {
		isRetryable := retrier.IsRetryableError(err)
		return 0, retrier.NewRetryableError(err, isRetryable, "upload_part", 0)
	}

	ugw.resumeOff = chunkEnd
	return size, nil
}

func (ugw *UnreliableGCSWriter) GetResumeOffset(ctx context.Context) (int64, error) {
	if ugw.isAborted {
		return 0, errors.New("operation aborted")
	}

	offset, complete, err := ugw.gcsClient.GetResumeOffset(ctx, ugw.uploadUrl)
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
	ugw.gcsClient.CancelUpload(ctx, ugw.uploadUrl)
}
