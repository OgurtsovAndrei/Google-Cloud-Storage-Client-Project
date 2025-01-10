package writers

import (
	"awesomeProject/proxy"
	"awesomeProject/utils"
	"context"
	"io"
	"log"
	"strings"
)

type UnreliableProxyWriter struct {
	cg     *proxy.ClientConnectionGroup
	bucket string
	object string
}

func NewUnreliableProxyWriter(ctx context.Context, cg *proxy.ClientConnectionGroup, bucket, object string) (*UnreliableProxyWriter, error) {
	w := &UnreliableProxyWriter{
		cg:     cg,
		bucket: bucket,
		object: object,
	}

	req := &proxy.InitUploadSessionRequest{
		Header: proxy.RequestHeader{
			RequestUid:  cg.NextUid(),
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
	log.Printf("Sending init connection message...")

	cg.CreateRespondChannel(req.Header.RequestUid)
	if err := cg.SendMessage(ctx, &msg); err != nil {
		return nil, &utils.Error{
			Code:  utils.ErrCodeSendMessage,
			Msg:   "Failed to send init connection message",
			Cause: err,
			Tags:  []string{utils.TagNetwork, utils.TagRetryable},
		}
	}

	resp, err := w.cg.WaitResponse(ctx, req.Header.RequestUid, req.Header.RequestType)
	if err != nil {
		return nil, &utils.Error{
			Code:  utils.ErrCodeWaitResponse,
			Msg:   "Failed to receive init connection response",
			Cause: err,
			Tags:  []string{utils.TagNetwork, utils.TagRetryable},
		}
	}

	if resp.IsErr() {
		customErr, convErr := resp.AsErr()
		if convErr == nil {
			return nil, customErr
		}
		return nil, &utils.Error{
			Code:  utils.ErrCodeInitSession,
			Msg:   "Failed to deserialize server error response",
			Cause: convErr,
			Tags:  []string{utils.TagInternal},
		}
	}

	return w, nil
}

func (w *UnreliableProxyWriter) WriteAt(
	ctx context.Context,
	chunkBegin, chunkEnd int64,
	reader *ScatterGatherBuffer,
	isLast bool,
) (int64, *utils.Error) {

	var maxPartSize uint32 = 1 * 1024 * 1024
	parts := reader.SplitByParts(maxPartSize)
	requestId := w.cg.NextUid()

	var off int64 = chunkBegin
	partCtx, cancel := context.WithCancel(ctx)
	defer func() {
		log.Printf("CLIENT: WriteAt: Context cancelled for request with ID %d", requestId)
		cancel()
	}()

	w.cg.CreateRespondChannel(requestId)

	go func() {
		log.Printf("CLIENT: WriteAt: Sending %d parts for request with ID %d", len(parts), requestId)
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

			err := w.cg.SendMessage(partCtx, &message)
			if err != nil {
				ResMessage := proxy.BuildErrorResponse(req.Header.RequestUid, err)
				w.cg.DispatchResponse(ResMessage)
			}
		}
	}()

	resp, err := w.cg.WaitResponse(ctx, requestId, proxy.MessageTypeUploadPart)
	if err != nil {
		return 0, err
	}

	if resp.IsErr() {
		customErr, convErr := resp.AsErr()
		if convErr == nil {
			return 0, customErr
		}
		return 0, convErr
	}

	return chunkEnd - chunkBegin, nil
}

func (w *UnreliableProxyWriter) GetResumeOffset(ctx context.Context) (int64, *utils.Error) {
	req := &proxy.GetResumeOffsetRequest{
		Header: proxy.RequestHeader{
			RequestUid:  w.cg.NextUid(),
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
	w.cg.CreateRespondChannel(req.Header.RequestUid)
	if err := w.cg.SendMessage(ctx, &message); err != nil {
		return 0, &utils.Error{
			Code:  utils.ErrCodeSendMessage,
			Msg:   "Failed to send resume offset request",
			Cause: err,
			Tags:  []string{utils.TagNetwork, utils.TagRetryable},
		}
	}

	resp, err := w.cg.WaitResponse(ctx, req.Header.RequestUid, req.Header.RequestType)
	if err != nil {
		return 0, &utils.Error{
			Code:  utils.ErrCodeWaitResponse,
			Msg:   "Failed to get resume offset response",
			Cause: err,
			Tags:  []string{utils.TagNetwork, utils.TagRetryable},
		}
	}

	if resp.IsErr() {
		customErr, convErr := resp.AsErr()
		if convErr == nil {
			return 0, customErr
		}
		return 0, convErr
	}

	off, parseErr := parseOffset(strings.NewReader(resp.Data))
	if parseErr != nil {
		return 0, parseErr
	}

	return off, nil
}

func (w *UnreliableProxyWriter) Abort(ctx context.Context) {
	req := &proxy.AbortRequest{
		Header: proxy.RequestHeader{
			RequestUid:  w.cg.NextUid(),
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
	w.cg.CreateRespondChannel(req.Header.RequestUid)
	_ = w.cg.SendMessage(ctx, &message)
	_, _ = w.cg.WaitResponse(ctx, req.Header.RequestUid, req.Header.RequestType)
}

func parseOffset(data io.Reader) (int64, *utils.Error) {
	var offset int64
	buf := make([]byte, 8)
	if _, err := data.Read(buf); err != nil {
		return 0, &utils.Error{
			Code:  utils.ErrCodeParseOffset,
			Msg:   "Failed to read offset from response data",
			Cause: err,
			Tags:  []string{utils.TagIllegalArgument},
		}
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
