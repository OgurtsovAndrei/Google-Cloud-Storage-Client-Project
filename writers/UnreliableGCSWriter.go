package writers

import (
	"awesomeProject/utils"
	"context"
	"errors"
	"fmt"
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

func NewUnreliableGCSWriter(ctx context.Context, bucket, objectName string) (*UnreliableGCSWriter, error) {
	gcsClient, err := utils.NewGcsClient(ctx)
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

func (ugw *UnreliableGCSWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, buf []byte, isLast bool) (int64, error) {

	if ugw.isAborted {
		return 0, errors.New("operation aborted")
	}
	if chunkBegin != ugw.resumeOff {
		msg := fmt.Sprintf("WriteAt called on chunkBegin %d, but resumeOff is %d", chunkBegin, ugw.resumeOff)
		fmt.Printf(msg)
		return 0, errors.New(msg)
	}
	if chunkEnd-chunkBegin != int64(len(buf)) {
		return 0, errors.New("buffer size does not match chunk range")
	}

	writeStart := time.Now()
	err := ugw.gcsClient.UploadObjectPart(ctx, ugw.uploadUrl, chunkBegin, buf, isLast)
	writeDuration := time.Since(writeStart).Seconds()

	if err != nil {
		ugw.resumeOff = chunkBegin
		return 0, err
	}

	uploadSpeed := float64(len(buf)) / writeDuration / (1024 * 1024) // MB/s
	fmt.Printf("Uploaded %d bytes at offset %d with speed %.2f MB/s\n", len(buf), chunkBegin, uploadSpeed)
	ugw.resumeOff = chunkBegin + int64(len(buf))

	return int64(len(buf)), nil
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
