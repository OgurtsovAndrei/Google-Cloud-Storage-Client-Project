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
		return 0, &retrier.GCSError{Code: 499, Message: "operation aborted"}
	}

	const MinUploadChunkSize = 256 * 1024

	currentOffset, complete, err := ugw.gcsClient.GetResumeOffset(ctx, ugw.uploadUrl)
	if err != nil {
		isRetryable := retrier.IsRetryableError(err)
		return 0, retrier.NewRetryableError(err, isRetryable, "get_resume_offset", 0)
	}
	if complete {
		ugw.resumeOff = currentOffset
		return 0, nil
	}

	ugw.resumeOff = currentOffset

	if chunkBegin != ugw.resumeOff {
		return 0, &retrier.GCSError{
			Code:    400,
			Message: fmt.Sprintf("invalid offset: expected %d, got %d", ugw.resumeOff, chunkBegin),
		}
	}

	size := chunkEnd - chunkBegin

	if !isLast {
		// align the offset to nearest MinUploadChunkSize boundary
		alignedBegin := (chunkBegin + MinUploadChunkSize - 1) / MinUploadChunkSize * MinUploadChunkSize

		if chunkBegin < alignedBegin {
			skipBytes := alignedBegin - chunkBegin
			if skipBytes > size {
				skipBytes = size
			}
			skipBuf := make([]byte, skipBytes)
			n, err := io.ReadFull(reader, skipBuf)
			if err != nil {
				return int64(n), err
			}
			ugw.resumeOff += int64(n)
			return int64(n), nil
		}

		remainingSize := chunkEnd - alignedBegin
		alignedSize := (remainingSize / MinUploadChunkSize) * MinUploadChunkSize
		if alignedSize == 0 {
			return 0, nil
		}
		chunkEnd = alignedBegin + alignedSize
		size = chunkEnd - chunkBegin
	}

	written, err := ugw.gcsClient.UploadObjectPart(ctx, ugw.uploadUrl, chunkBegin, reader, size, isLast)
	if err != nil {
		isRetryable := retrier.IsRetryableError(err)
		currentOffset, _, offsetErr := ugw.gcsClient.GetResumeOffset(ctx, ugw.uploadUrl)
		if offsetErr == nil {
			written = currentOffset - chunkBegin
			if written < 0 {
				written = 0
			}
			ugw.resumeOff = currentOffset
		} else if written > 0 {
			ugw.resumeOff = chunkBegin + written
		}
		return written, retrier.NewRetryableError(err, isRetryable, "upload_part", 0)
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
