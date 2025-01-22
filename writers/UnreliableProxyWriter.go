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
	cg     *proxy.ClientConnectionGroup
	bucket string
	object string
	uid    uint32
}

func NewUnreliableProxyWriter(ctx context.Context, cg *proxy.ClientConnectionGroup, bucket, object string) (*UnreliableProxyWriter, error) {
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

//func (upw *UnreliableProxyWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader io.Reader, isLast bool) (int64, error) {
//	if upw.isAborted {
//		return 0, errors.New("operation aborted")
//	}
//	if chunkBegin != upw.currentOffset {
//		msg := fmt.Sprintf("WriteAt called on chunkBegin %d, but currentOffset is %d", chunkBegin, upw.currentOffset)
//		fmt.Println(msg)
//		return 0, errors.New(msg)
//	}
//	size := chunkEnd - chunkBegin
//	if size <= 0 {
//		return 0, errors.New("invalid chunk size")
//	}
//
//	upw.sequenceNumber++
//
//	header := proxy.RequestHeader{
//		RequestUid:  upw.sequenceNumber,
//		RequestType: proxy.MessageTypeUploadPart,
//	}
//
//	writeAtReq := proxy.WriteAtRequestHeader{
//		ChunkBegin: chunkBegin,
//		ChunkEnd:   chunkEnd,
//		IsLast:     boolToByte(isLast),
//	}
//
//	reqSize := binary.Size(writeAtReq) + int(size)
//	header.RequestSize = uint32(reqSize)
//
//	buf := new(bytes.Buffer)
//	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
//		upw.currentOffset = chunkBegin
//		return 0, fmt.Errorf("failed to write request header: %w", err)
//	}
//	if err := binary.Write(buf, binary.BigEndian, &writeAtReq); err != nil {
//		upw.currentOffset = chunkBegin
//		return 0, fmt.Errorf("failed to write WriteAtRequestHeader: %w", err)
//	}
//
//	conn := upw.connection
//	if _, err := conn.Write(buf.Bytes()); err != nil {
//		upw.currentOffset = chunkBegin
//		return 0, fmt.Errorf("failed to write request metadata: %w", err)
//	}
//
//	startTime := time.Now()
//	n, err := io.CopyN(conn, reader, size)
//	if err != nil {
//		upw.currentOffset = chunkBegin
//		return n, fmt.Errorf("failed to write data: %w", err)
//	}
//
//	if err := upw.receiveResponse(); err != nil {
//		return n, err
//	}
//
//	elapsedTime := time.Since(startTime)
//	uploadSpeed := float64(n) / elapsedTime.Seconds()
//	fmt.Printf("Sent chunk to TCP [%d - %d] (%d bytes) in %.2f seconds (%.2f MB/s)\n",
//		chunkBegin, chunkEnd, n, elapsedTime.Seconds(), uploadSpeed/(1024*1024))
//
//	upw.currentOffset = chunkEnd
//	return n, nil
//}

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

//func (upw *UnreliableProxyWriter) receiveResponse() error {
//	var resp proxy.ResponseHeader
//	if err := binary.Read(upw.connection, binary.BigEndian, &resp); err != nil {
//		return fmt.Errorf("failed to read response header: %w", err)
//	}
//
//	if resp.MessageLength != 0 {
//		message := make([]byte, resp.MessageLength)
//		if _, err := io.ReadFull(upw.connection, message); err != nil {
//			return fmt.Errorf("failed to read error message: %w", err)
//		}
//		if resp.StatusCode != 0 {
//			return fmt.Errorf("error from proxy: %s", string(message))
//		} else {
//			fmt.Println(string(message))
//		}
//	}
//
//	return nil
//}

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
