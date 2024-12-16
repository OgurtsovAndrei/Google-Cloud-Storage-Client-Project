package writers

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"sync/atomic"

	"awesomeProject/proxy"
)

type UnreliableProxyWriter struct {
	cg     *proxy.ConnectionGroup
	bucket string
	object string
	uid    uint32
}

func NewUnreliableProxyWriter(ctx context.Context, cg *proxy.ConnectionGroup, bucket, object string) (*UnreliableProxyWriter, error) {
	w := &UnreliableProxyWriter{
		cg:     cg,
		bucket: bucket,
		object: object,
	}
	req := &proxy.InitUploadSessionRequest{
		Header: proxy.RequestHeader{
			RequestUid:  atomic.AddUint32(&w.uid, 1),
			RequestType: proxy.MessageTypeInitConnection,
		},
		InitUploadSessionHeader: proxy.InitUploadSessionHeader{
			BucketNameLength: uint32(len(bucket)),
			ObjectNameLength: uint32(len(object)),
		},
		Bucket: bucket,
		Object: object,
	}
	msg := req.ToRequestMessage()
	log.Printf("Sending init connetction message...")
	if err := cg.SendMessage(ctx, &msg); err != nil {
		return nil, err
	}
	log.Printf("Waiting for init connetction response...")
	resp, err := w.cg.WaitResponse(ctx, req.Header.RequestUid)
	if err != nil {
		return nil, err
	}
	if resp.Header.StatusCode != 0 {
		return nil, errors.New("failed to initialize upload session")
	}
	return w, nil
}

func (w *UnreliableProxyWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader *ScatterGatherBuffer, isLast bool) (int64, error) {
	var maxPartSize uint32 = 1 * 1024 * 1024
	parts := reader.SplitByParts(maxPartSize)
	requestId := atomic.AddUint32(&w.uid, 1)

	var off int64 = chunkBegin
	for _, part := range parts {
		req := &proxy.WriteAtRequest{
			Header: proxy.RequestHeader{
				RequestUid:  requestId,
				RequestType: proxy.MessageTypeUploadPart,
			},
			WriteAtHeader: proxy.WriteAtHeader{
				BucketNameLength: uint32(len(w.bucket)),
				ObjectNameLength: uint32(len(w.object)),
				ChunkBegin:       chunkBegin,
				ChunkEnd:         chunkEnd,
				Off:              off,
				Size:             int64(part.size),
				IsLast:           boolToByte(isLast),
			},
			Bucket: w.bucket,
			Object: w.object,
			Data:   part,
		}

		off += int64(part.size)
		message := req.ToRequestMessage()
		if err := w.cg.SendMessage(ctx, &message); err != nil {
			return 0, err
		}
	}

	resp, err := w.cg.WaitResponse(ctx, requestId)
	if err != nil {
		return 0, err
	}
	if resp.Header.StatusCode != 0 {
		return 0, errors.New("failed to write data to proxy")
	}
	return chunkEnd - chunkBegin, nil
}

func (w *UnreliableProxyWriter) GetResumeOffset(ctx context.Context) (int64, error) {
	req := &proxy.GetResumeOffsetRequest{
		Header: proxy.RequestHeader{
			RequestUid:  atomic.AddUint32(&w.uid, 1),
			RequestType: proxy.MessageTypeGetResumeOffset,
		},
		GetResumeOffsetHeader: proxy.GetResumeOffsetHeader{
			BucketNameLength: uint32(len(w.bucket)),
			ObjectNameLength: uint32(len(w.object)),
		},
		Bucket: w.bucket,
		Object: w.object,
	}
	message := req.ToRequestMessage()
	if err := w.cg.SendMessage(ctx, &message); err != nil {
		return 0, err
	}
	resp, err := w.cg.WaitResponse(ctx, req.Header.RequestUid)
	if err != nil {
		return 0, err
	}
	if resp.Header.StatusCode != 0 {
		return 0, errors.New("failed to get resume offset")
	}
	return parseOffset(strings.NewReader(resp.Data))
}

func (w *UnreliableProxyWriter) Abort(ctx context.Context) {
	req := &proxy.AbortRequest{
		Header: proxy.RequestHeader{
			RequestUid:  atomic.AddUint32(&w.uid, 1),
			RequestType: proxy.MessageTypeAbort,
		},
		AbortHeader: proxy.AbortHeader{
			BucketNameLength: uint32(len(w.bucket)),
			ObjectNameLength: uint32(len(w.object)),
		},
		Bucket: w.bucket,
		Object: w.object,
	}
	message := req.ToRequestMessage()
	_ = w.cg.SendMessage(ctx, &message)
	_, _ = w.cg.WaitResponse(ctx, req.Header.RequestUid)
}

func parseOffset(data io.Reader) (int64, error) {
	var offset int64
	buf := make([]byte, 8)
	if _, err := data.Read(buf); err != nil {
		return 0, err
	}
	for _, b := range buf {
		offset = offset*10 + int64(b-'0')
	}
	return offset, nil
}

func boolToByte(val bool) byte {
	if val {
		return 1
	}
	return 0
}
