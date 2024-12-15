package writers

import (
	"awesomeProject/proxy"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
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
}

func NewUnreliableProxyWriter(proxyAddress, bucket, objectName string) (*UnreliableProxyWriter, error) {
	conn, err := net.Dial("tcp", proxyAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to GCSProxyServer: %w", err)
	}

	upw := &UnreliableProxyWriter{
		connection:     conn,
		bucket:         bucket,
		objectName:     objectName,
		currentOffset:  0,
		isAborted:      false,
		sequenceNumber: 0,
	}

	if err := upw.sendInitConnectionRequest(); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to initialize connection: %w", err)
	}

	return upw, nil
}

func (upw *UnreliableProxyWriter) sendInitConnectionRequest() error {
	upw.sequenceNumber++
	header := proxy.RequestHeader{
		RequestUid:  upw.sequenceNumber,
		RequestType: proxy.MessageTypeInitConnection,
	}

	bucketNameBytes := []byte(upw.bucket)
	objectNameBytes := []byte(upw.objectName)

	initReq := proxy.InitUploadSessionHeader{
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
		return fmt.Errorf("failed to write InitUploadSessionHeader: %w", err)
	}
	buf.Write(bucketNameBytes)
	buf.Write(objectNameBytes)

	if _, err := upw.connection.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to send init connection request: %w", err)
	}

	return upw.receiveResponse()
}

func (upw *UnreliableProxyWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader io.Reader, isLast bool) (int64, error) {
	if upw.isAborted {
		return 0, errors.New("operation aborted")
	}
	if chunkBegin != upw.currentOffset {
		msg := fmt.Sprintf("WriteAt called on chunkBegin %d, but currentOffset is %d", chunkBegin, upw.currentOffset)
		fmt.Println(msg)
		return 0, errors.New(msg)
	}
	size := chunkEnd - chunkBegin
	if size <= 0 {
		return 0, errors.New("invalid chunk size")
	}

	upw.sequenceNumber++

	header := proxy.RequestHeader{
		RequestUid:  upw.sequenceNumber,
		RequestType: proxy.MessageTypeUploadPart,
	}

	writeAtReq := proxy.WriteAtRequestHeader{
		ChunkBegin: chunkBegin,
		ChunkEnd:   chunkEnd,
		IsLast:     boolToByte(isLast),
	}

	reqSize := binary.Size(writeAtReq) + int(size)
	header.RequestSize = uint32(reqSize)

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
		upw.currentOffset = chunkBegin
		return 0, fmt.Errorf("failed to write request header: %w", err)
	}
	if err := binary.Write(buf, binary.BigEndian, &writeAtReq); err != nil {
		upw.currentOffset = chunkBegin
		return 0, fmt.Errorf("failed to write WriteAtRequestHeader: %w", err)
	}

	conn := upw.connection
	if _, err := conn.Write(buf.Bytes()); err != nil {
		upw.currentOffset = chunkBegin
		return 0, fmt.Errorf("failed to write request metadata: %w", err)
	}

	startTime := time.Now()
	n, err := io.CopyN(conn, reader, size)
	if err != nil {
		upw.currentOffset = chunkBegin
		return n, fmt.Errorf("failed to write data: %w", err)
	}

	if err := upw.receiveResponse(); err != nil {
		return n, err
	}

	elapsedTime := time.Since(startTime)
	uploadSpeed := float64(n) / elapsedTime.Seconds()
	fmt.Printf("Sent chunk to TCP [%d - %d] (%d bytes) in %.2f seconds (%.2f MB/s)\n",
		chunkBegin, chunkEnd, n, elapsedTime.Seconds(), uploadSpeed/(1024*1024))

	upw.currentOffset = chunkEnd
	return n, nil
}

func (upw *UnreliableProxyWriter) GetResumeOffset(ctx context.Context) (int64, error) {
	if upw.isAborted {
		return 0, errors.New("operation aborted")
	}
	upw.sequenceNumber++

	header := proxy.RequestHeader{
		RequestUid:  upw.sequenceNumber,
		RequestType: proxy.MessageTypeGetResumeOffset,
		RequestSize: 0,
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
		return 0, fmt.Errorf("failed to write request header: %w", err)
	}

	conn := upw.connection
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return 0, fmt.Errorf("failed to write GetResumeOffset request: %w", err)
	}

	var resumeOffset int64
	if err := binary.Read(conn, binary.BigEndian, &resumeOffset); err != nil {
		return 0, fmt.Errorf("failed to read resume offset: %w", err)
	}

	if err := upw.receiveResponse(); err != nil {
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
	upw.sequenceNumber++

	header := proxy.RequestHeader{
		RequestUid:  upw.sequenceNumber,
		RequestType: proxy.MessageTypeAbort,
		RequestSize: 0,
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
		fmt.Println("Error writing abort request header:", err)
		return
	}

	conn := upw.connection
	if _, err := conn.Write(buf.Bytes()); err != nil {
		fmt.Println("Error sending abort request:", err)
		return
	}

	if err := upw.receiveResponse(); err != nil {
		fmt.Printf("Error in abort response: %v\n", err)
	}
}

func (upw *UnreliableProxyWriter) receiveResponse() error {
	var resp proxy.ResponseHeader
	if err := binary.Read(upw.connection, binary.BigEndian, &resp); err != nil {
		return fmt.Errorf("failed to read response header: %w", err)
	}

	if resp.MessageLength != 0 {
		message := make([]byte, resp.MessageLength)
		if _, err := io.ReadFull(upw.connection, message); err != nil {
			return fmt.Errorf("failed to read error message: %w", err)
		}
		if resp.StatusCode != 0 {
			return fmt.Errorf("error from proxy: %s", string(message))
		} else {
			fmt.Println(string(message))
		}
	}

	return nil
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
