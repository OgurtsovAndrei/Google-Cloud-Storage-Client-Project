package writers

import (
	"awesomeProject/retrier"
	"awesomeProject/utils"
	"bytes"
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

func NewUnreliableGCSWriter(ctx context.Context, bucket, objectName string, injector utils.ErrorInjector) (*UnreliableGCSWriter, error) {
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

	if chunkBegin != ugw.resumeOff {
		return 0, &retrier.GCSError{
			Code:    400,
			Message: fmt.Sprintf("invalid offset: expected %d, got %d", ugw.resumeOff, chunkBegin),
		}
	}

	size := chunkEnd - chunkBegin

	// for testing: if we have a hook, read the data and call the hook
	if ugw.writeHook != nil {
		data, err := io.ReadAll(reader)
		if err != nil {
			return 0, err
		}
		ugw.writeHook(data, chunkBegin)
		reader = bytes.NewReader(data)
	}

	err := ugw.gcsClient.UploadObjectPart(ctx, ugw.uploadUrl, chunkBegin, reader, size, isLast)
	if err != nil {
		return 0, err
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
