package writers

import (
	"awesomeProject/proxy"
	"awesomeProject/utils"
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
	connections    []net.Conn
	resumeConn     net.Conn
	abortConn      net.Conn
	bucket         string
	objectName     string
	currentOffset  int64
	isAborted      bool
	sequenceNumber uint32
	nConnections   uint32
}

func NewUnreliableProxyWriter(proxyAddress, bucket, objectName string, nConnections uint32) (*UnreliableProxyWriter, error) {
	connections := make([]net.Conn, nConnections)
	for i := range connections {
		conn, err := net.Dial("tcp", proxyAddress)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to GCSProxyServer: %w", err)
		}
		connections[i] = conn
	}

	resumeConn, err := net.Dial("tcp", proxyAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to create resume connection: %w", err)
	}

	abortConn, err := net.Dial("tcp", proxyAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to create abort connection: %w", err)
	}

	upw := &UnreliableProxyWriter{
		connections:    connections,
		resumeConn:     resumeConn,
		abortConn:      abortConn,
		bucket:         bucket,
		objectName:     objectName,
		currentOffset:  0,
		isAborted:      false,
		sequenceNumber: 0,
		nConnections:   nConnections,
	}

	if err := upw.initAllConnections(); err != nil {
		upw.Close()
		return nil, fmt.Errorf("failed to initialize connections: %w", err)
	}

	return upw, nil
}

func (upw *UnreliableProxyWriter) initAllConnections() error {
	for _, conn := range upw.connections {
		if err := upw.initConnection(conn); err != nil {
			return err
		}
	}
	if err := upw.initConnection(upw.resumeConn); err != nil {
		return err
	}
	if err := upw.initConnection(upw.abortConn); err != nil {
		return err
	}
	return nil
}

func (upw *UnreliableProxyWriter) initConnection(conn net.Conn) error {
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

	if _, err := conn.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to send init connection request: %w", err)
	}

	if code, msg := upw.receiveResponse(conn); code != 0 {
		return fmt.Errorf("error in init connection: %s", msg)
	}
	return nil
}

func (upw *UnreliableProxyWriter) WriteAt(ctx context.Context, chunkBegin, chunkEnd int64, reader *utils.ScatterGatherBuffer, isLast bool) (int64, error) {
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

	partSize := uint32(size) / upw.nConnections

	var wg sync.WaitGroup
	results := make(chan int64, upw.nConnections)
	errorsChan := make(chan error, upw.nConnections)

	start := uint32(chunkBegin)
	for i := uint32(0); i < upw.nConnections; i++ {
		part, _ := reader.TakeBytesUnsafe(partSize)
		if part.Size() != partSize {
			println("WTF")
		}

		upw.sequenceNumber++

		header := proxy.RequestHeader{
			SequenceNumber: upw.sequenceNumber,
			RequestType:    proxy.MessageTypeUploadPart,
		}

		writeAtReq := proxy.WriteAtRequestHeader{
			ChunkBegin: chunkBegin,
			ChunkEnd:   chunkEnd,
			Off:        int64(start),
			Size:       int64(part.Size()),
			IsLast:     boolToByte(isLast),
		}

		wg.Add(1)
		go func(conn net.Conn, hdr proxy.RequestHeader, req proxy.WriteAtRequestHeader, p io.Reader) {
			defer wg.Done()
			n, err := upw.sentPartToTcp(conn, hdr, req, p)
			if err != nil {
				println(err.Error())
				errorsChan <- err
				return
			}
			results <- n
		}(upw.connections[i], header, writeAtReq, part)

		start = start + partSize
	}

	go func() {
		wg.Wait()
		close(results)
		close(errorsChan)
	}()

	var totalWritten int64
	for r := range results {
		totalWritten += r
	}

	select {
	case err := <-errorsChan:
		return 0, err
	default:
	}

	upw.currentOffset = chunkEnd
	return totalWritten, nil
}

func (upw *UnreliableProxyWriter) sentPartToTcp(
	conn net.Conn,
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

	if _, err := conn.Write(buf.Bytes()); err != nil {
		return 0, fmt.Errorf("failed to write request metadata: %w", err)
	}

	startTime := time.Now()
	n, err := io.CopyN(conn, reader, writeAtReq.Size)
	if err != nil {
		return n, fmt.Errorf("failed to write data: %w", err)
	}

	if code, msg := upw.receiveResponse(conn); code != 0 {
		fmt.Printf("Error in write at response: %s\n", msg)
		return n, fmt.Errorf("failed to write data: code: %d, msg: %s", code, msg)
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

	if _, err := upw.resumeConn.Write(buf.Bytes()); err != nil {
		return 0, fmt.Errorf("failed to write GetResumeOffset request: %w", err)
	}

	var resumeOffset int64
	if err := binary.Read(upw.resumeConn, binary.BigEndian, &resumeOffset); err != nil {
		return 0, fmt.Errorf("failed to read resume offset: %w", err)
	}

	if code, msg := upw.receiveResponse(upw.resumeConn); code != 0 {
		return 0, fmt.Errorf("error in get resume offset: %s", msg)
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

	if _, err := upw.abortConn.Write(buf.Bytes()); err != nil {
		fmt.Println("Error sending abort request:", err)
		return
	}

	if code, msg := upw.receiveResponse(upw.abortConn); code != 0 {
		fmt.Printf("Error in abort response: %s\n", msg)
	}
}

func (upw *UnreliableProxyWriter) receiveResponse(conn net.Conn) (uint32, string) {
	var resp proxy.ResponseHeader
	if err := binary.Read(conn, binary.BigEndian, &resp); err != nil {
		return 1, fmt.Sprintf("failed to read response header: %s", err.Error())
	}

	message := make([]byte, resp.MessageLength)
	if _, err := io.ReadFull(conn, message); err != nil {
		return 1, fmt.Sprintf("failed to read response message: %s", err.Error())
	}
	return resp.StatusCode, string(message)
}

func (upw *UnreliableProxyWriter) Close() {
	for _, conn := range upw.connections {
		_ = conn.Close()
	}
	_ = upw.resumeConn.Close()
	_ = upw.abortConn.Close()
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
