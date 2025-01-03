package writers

import (
	"awesomeProject/netUtils"
	"awesomeProject/utils"
	"context"
	"fmt"
	"net/http"
	"time"
)

type UnreliableGCSWriter struct {
	gcsClient  *utils.GcsClient
	uploadUrl  string
	resumeOff  int64
	isAborted  bool
	bucket     string
	objectName string
}

var (
	checkCancelConnThreshold int64 = 1 * 1024 * 1024
)

func NewUnreliableGCSWriter(ctx context.Context, bucket, objectName string) (*UnreliableGCSWriter, error) {
	customTransport := &http.Transport{
		DialContext: netUtils.NewUnstableDialer(checkCancelConnThreshold).DialContext,
	}
	gcsClient, err := utils.NewGcsClientWithCustomTransport(ctx, customTransport)
	//gcsClient, err := utils.NewGcsClient(ctx)
	if err != nil {
		return nil, &utils.Error{
			Code:  utils.ErrCodeInitSession,
			Msg:   "Failed to create GCS client",
			Cause: err,
			Tags:  []string{utils.TagNetwork, utils.TagRetryable},
		}
	}

	uploadUrl, err := gcsClient.NewUploadSession(ctx, bucket, objectName)
	if err != nil {
		return nil, &utils.Error{
			Code:  utils.ErrCodeInitSession,
			Msg:   "Failed to create upload session",
			Cause: err,
			Tags:  []string{utils.TagNetwork, utils.TagRetryable},
		}
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

func (ugw *UnreliableGCSWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader *ScatterGatherBuffer, isLast bool) (int64, *utils.Error) {
	if ugw.isAborted {
		return 0, &utils.Error{
			Code: utils.ErrCodeAbortFailed,
			Msg:  "Write operation aborted",
			Tags: []string{utils.TagIllegalArgument},
		}
	}

	if chunkBegin != ugw.resumeOff {
		msg := fmt.Sprintf("WriteAt called on chunkBegin %d, but resumeOff is %d", chunkBegin, ugw.resumeOff)
		return 0, &utils.Error{
			Code: utils.ErrCodeOutOfOrderWrite,
			Msg:  msg,
			Tags: []string{utils.TagOutOfOrder},
		}
	}

	size := chunkEnd - chunkBegin
	writeStart := time.Now()
	err := ugw.gcsClient.UploadObjectPart(ctx, ugw.uploadUrl, chunkBegin, reader, size, isLast)
	writeDuration := time.Since(writeStart).Seconds()

	if err != nil {
		ugw.resumeOff = chunkBegin
		return 0, &utils.Error{
			Code:  utils.ErrCodeUploadChunkFailed,
			Msg:   "Failed to upload object part",
			Cause: err,
			Tags:  []string{utils.TagNetwork, utils.TagRetryable},
		}
	}

	uploadSpeed := float64(size) / writeDuration / (1024 * 1024) // MB/s
	fmt.Printf("Uploaded %d bytes at offset %d with speed %.2f MB/s\n", size, chunkBegin, uploadSpeed)
	ugw.resumeOff = chunkBegin + int64(size)

	return size, nil
}

func (ugw *UnreliableGCSWriter) GetResumeOffset(ctx context.Context) (int64, *utils.Error) {
	if ugw.isAborted {
		return 0, &utils.Error{
			Code: utils.ErrCodeAbortFailed,
			Msg:  "Operation aborted",
			Tags: []string{utils.TagIllegalArgument},
		}
	}

	offset, complete, err := ugw.gcsClient.GetResumeOffset(ctx, ugw.uploadUrl)
	if err != nil {
		return 0, &utils.Error{
			Code:  utils.ErrCodeGetResume,
			Msg:   "Failed to get resume offset",
			Cause: err,
			Tags:  []string{utils.TagNetwork, utils.TagRetryable},
		}
	}

	if complete {
		return offset, nil
	}

	ugw.resumeOff = offset
	return ugw.resumeOff, nil
}

func (ugw *UnreliableGCSWriter) Abort(ctx context.Context) {
	ugw.isAborted = true
	err := ugw.gcsClient.CancelUpload(ctx, ugw.uploadUrl)
	if err != nil {
		fmt.Printf("Error cancelling upload session: %v\n", err)
	}
}
