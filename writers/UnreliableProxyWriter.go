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
	"sync"
	"time"
)

type UnreliableProxyWriter struct {
	connection     net.Conn
	bucket         string
	objectName     string
	currentOffset  int64
	isAborted      bool
	sequenceNumber uint32
	nConnections   uint32
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
		nConnections:   2,
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
		SequenceNumber: upw.sequenceNumber,
		RequestType:    proxy.MessageTypeInitConnection,
	}

	bucketNameBytes := []byte(upw.bucket)
	objectNameBytes := []byte(upw.objectName)

	initReq := proxy.InitConnectionRequestHeader{
		BucketNameLength: uint32(len(bucketNameBytes)),
		ObjectNameLength: uint32(len(objectNameBytes)),
	}

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
		return fmt.Errorf("failed to write request header: %w", err)
	}
	if err := binary.Write(buf, binary.BigEndian, &initReq); err != nil {
		return fmt.Errorf("failed to write InitConnectionRequestHeader: %w", err)
	}
	buf.Write(bucketNameBytes)
	buf.Write(objectNameBytes)

	if _, err := upw.connection.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to send init connection request: %w", err)
	}

	if code, msg := upw.receiveResponse(); code != 0 {
		fmt.Printf("Error in init connection response: %s\n", msg)
		return fmt.Errorf("error in init connection: %s", msg)
	}
	return nil
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

	var wg sync.WaitGroup

	start := 0
	for i := 0; i < ; i++ {
		end := start + partSize
		if i == n-1 {
			end += remainder // Add the remainder to the last part
		}

		part := chunk[start:end] // Slice the array for this part

		wg.Add(1)
		go func(p []byte) {
			defer wg.Done()
			process(p)
		}(part)

		start = end
	}

	wg.Wait()
	fmt.Println("All parts processed")

	upw.sequenceNumber++

	header := proxy.RequestHeader{
		SequenceNumber: upw.sequenceNumber,
		RequestType:    proxy.MessageTypeUploadPart,
	}

	writeAtReq := proxy.WriteAtRequestHeader{
		ChunkBegin: chunkBegin,
		ChunkEnd:   chunkEnd,
		Off:        chunkBegin,
		Size:       chunkEnd - chunkBegin,
		IsLast:     boolToByte(isLast),
	}

	upw.currentOffset = chunkEnd
	return chunkBegin - chunkEnd, nil
}

func (upw *UnreliableProxyWriter) sentPartToTcp(
	ctx context.Context,
	header proxy.RequestHeader,
	writeAtReq proxy.WriteAtRequestHeader,
	reader io.Reader,
) (int64, error) {

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, &header); err != nil {
		return 0, fmt.Errorf("failed to write request header: %w", err)
	}
	if err := binary.Write(buf, binary.BigEndian, &writeAtReq); err != nil {
		return 0, fmt.Errorf("failed to write WriteAtRequestHeader: %w", err)
	}

	conn := upw.connection
	if _, err := conn.Write(buf.Bytes()); err != nil {
		return 0, fmt.Errorf("failed to write request metadata: %w", err)
	}

	startTime := time.Now()
	n, err := io.CopyN(conn, reader, writeAtReq.Size)
	if err != nil {
		return n, fmt.Errorf("failed to write data: %w", err)
	}

	if code, msg := upw.receiveResponse(); code != 0 {
		fmt.Printf("Error in write at response: %s\n", msg)
		return n, fmt.Errorf("failed to write data: code: %d, msg: %w", code, err)
	}

	//if code, msg := upw.receiveResponse(); code != 0 {
	//	fmt.Printf("Error in write at response: %s\n", msg)
	//	return n, fmt.Errorf("failed to write data: %w", err)
	//}

	elapsedTime := time.Since(startTime)
	uploadSpeed := float64(n) / elapsedTime.Seconds()
	fmt.Printf("Sent chunk to TCP [%d - %d] (%d bytes) in %.2f seconds (%.2f MB/s)\n",
		writeAtReq.Off, writeAtReq.Off+writeAtReq.Size, n, elapsedTime.Seconds(), uploadSpeed/(1024*1024))

	return n, nil
}

func (upw *UnreliableProxyWriter) GetResumeOffset(ctx context.Context) (int64, error) {
	if upw.isAborted {
		return 0, errors.New("operation aborted")
	}
	upw.sequenceNumber++

	header := proxy.RequestHeader{
		SequenceNumber: upw.sequenceNumber,
		RequestType:    proxy.MessageTypeGetResumeOffset,
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

	if code, msg := upw.receiveResponse(); code != 0 {
		fmt.Printf("Error in get resume off response: %s\n", msg)
		return 0, fmt.Errorf("error in get resume off: %s", msg)
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
		SequenceNumber: upw.sequenceNumber,
		RequestType:    proxy.MessageTypeAbort,
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

	if code, msg := upw.receiveResponse(); code != 0 {
		fmt.Printf("Error in abort response: %s\n", msg)
	}
}

func (upw *UnreliableProxyWriter) receiveResponse() (code uint32, msg string) {
	var resp proxy.ResponseHeader
	if err := binary.Read(upw.connection, binary.BigEndian, &resp); err != nil {
		return 1, fmt.Sprintf("failed to read response header: %s", err.Error())
	}

	message := make([]byte, resp.MessageLength)
	if _, err := io.ReadFull(upw.connection, message); err != nil {
		return 1, fmt.Sprintf("failed to read response message: %s", err.Error())
	}
	fmt.Printf("Response revived: %s\n", string(message))
	return resp.StatusCode, string(message)

}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
