package proxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"awesomeProject/utils"
)

type UploadSession struct {
	uploadUrl          string
	gcsClient          *utils.GcsClient
	isAborted          bool
	isCompleted        bool
	activeConnections  int
	bucketName         string
	objectName         string
	sessionCtx         context.Context
	cancelFunc         context.CancelFunc
	totalBytesUploaded int64
	uploadStartTime    time.Time
	uploadEndTime      time.Time

	chunkLock             sync.Mutex
	chunkWrittenEventChan chan struct{}
	currentChunkBeginOff  int64
	currentChunk          *utils.BuildableBuffer
}

func StartGCSProxyServer(ctx context.Context, listenAddress string) error {
	var (
		uploadSessions = make(map[string]*UploadSession)
		mutex          sync.Mutex
	)

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("error starting server: %w", err)
	}
	fmt.Printf("GCSProxyServer listening on %s\n", listenAddress)

	go func() {
		<-ctx.Done()
		_ = listener.Close()
		fmt.Println("GCSProxyServer has been shut down.")
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				fmt.Printf("Error accepting connection: %v\n", err)
				continue
			}
		}
		go handleConnection(ctx, conn, &uploadSessions, &mutex)
	}
}

func handleConnection(ctx context.Context, conn net.Conn, uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex) {
	defer conn.Close()

	var header RequestHeader
	if err := binary.Read(conn, binary.BigEndian, &header); err != nil {
		sendErrorResponse(conn, 0, fmt.Errorf("failed to read request header: %w", err))
		return
	}

	if header.RequestType != MessageTypeInitConnection {
		sendErrorResponse(conn, header.SequenceNumber, errors.New("first request must be init connection request"))
		return
	}

	session, sessionKey, err := handleInitConnection(ctx, conn, uploadSessions, uploadSessionsMutex)
	if err != nil {
		sendErrorResponse(conn, header.SequenceNumber, err)
		return
	}

	sendSuccessResponse(conn, header.SequenceNumber, "")

	for {
		var reqHeader RequestHeader
		if err := binary.Read(conn, binary.BigEndian, &reqHeader); err != nil {
			if err == io.EOF {
				break
			}
			sendErrorResponse(conn, 0, fmt.Errorf("failed to read request header: %w", err))
			return
		}

		var err error
		switch reqHeader.RequestType {
		case MessageTypeUploadPart:
			err = handleWriteAt(session.sessionCtx, conn, session, reqHeader)
		case MessageTypeGetResumeOffset:
			err = handleGetResumeOffset(conn, session, reqHeader)
		case MessageTypeAbort:
			err = handleAbort(session)
		default:
			err = fmt.Errorf("unknown request type: %d", reqHeader.RequestType)
		}

		if err != nil {
			sendErrorResponse(conn, reqHeader.SequenceNumber, err)
		} else {
			sendSuccessResponse(conn, reqHeader.SequenceNumber, fmt.Sprintf("OK for %s", RequestTypeToString(reqHeader.RequestType)))
		}
	}

	uploadSessionsMutex.Lock()
	session.activeConnections--
	if session.activeConnections == 0 && (session.isAborted || session.isCompleted) {
		session.cancelFunc()
		_ = session.gcsClient.CancelUpload(ctx, session.uploadUrl)
		delete(*uploadSessions, sessionKey)
	}
	uploadSessionsMutex.Unlock()
}

func handleInitConnection(ctx context.Context, conn net.Conn, uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex) (*UploadSession, string, error) {
	var initReq InitConnectionRequestHeader
	if err := binary.Read(conn, binary.BigEndian, &initReq); err != nil {
		return nil, "", fmt.Errorf("failed to read InitConnectionRequestHeader: %w", err)
	}

	bucketNameBytes := make([]byte, initReq.BucketNameLength)
	if _, err := io.ReadFull(conn, bucketNameBytes); err != nil {
		return nil, "", fmt.Errorf("failed to read bucket name: %w", err)
	}
	bucketName := string(bucketNameBytes)

	objectNameBytes := make([]byte, initReq.ObjectNameLength)
	if _, err := io.ReadFull(conn, objectNameBytes); err != nil {
		return nil, "", fmt.Errorf("failed to read object name: %w", err)
	}
	objectName := string(objectNameBytes)

	sessionKey := bucketName + "/" + objectName

	uploadSessionsMutex.Lock()
	defer uploadSessionsMutex.Unlock()

	session, exists := (*uploadSessions)[sessionKey]
	if !exists {
		var failed bool
		session, failed = createNewSession(ctx, uploadSessionsMutex, bucketName, objectName, uploadSessions, sessionKey)
		if failed {
			return nil, "", fmt.Errorf("failed to create a new session for %s/%s", bucketName, objectName)
		}
	} else {
		session.activeConnections++
	}

	return session, sessionKey, nil
}

func createNewSession(ctx context.Context, uploadSessionsMutex *sync.Mutex, bucketName string, objectName string, uploadSessions *map[string]*UploadSession, sessionKey string) (*UploadSession, bool) {
	sessionCtx, cancelFunc := context.WithTimeout(context.Background(), time.Hour)

	gcsClient, err := utils.NewGcsClient(sessionCtx)
	if err != nil {
		uploadSessionsMutex.Unlock()
		fmt.Printf("Failed to create GCS client: %v\n", err)
		cancelFunc()
		return nil, true
	}
	uploadUrl, err := gcsClient.NewUploadSession(sessionCtx, bucketName, objectName)
	if err != nil {
		uploadSessionsMutex.Unlock()
		fmt.Printf("Failed to create upload session: %v\n", err)
		cancelFunc()
		return nil, true
	}
	session := &UploadSession{
		uploadUrl:            uploadUrl,
		gcsClient:            gcsClient,
		currentChunkBeginOff: 0,
		isAborted:            false,
		isCompleted:          false,
		activeConnections:    1,
		bucketName:           bucketName,
		objectName:           objectName,
		sessionCtx:           sessionCtx,
		cancelFunc:           cancelFunc,
		uploadStartTime:      time.Now(),

		chunkWrittenEventChan: make(chan struct{}, 1),
	}
	(*uploadSessions)[sessionKey] = session

	go func() {
		<-sessionCtx.Done()
		uploadSessionsMutex.Lock()
		defer uploadSessionsMutex.Unlock()

		if _, ok := (*uploadSessions)[sessionKey]; !ok {
			return
		}

		session.isAborted = true
		fmt.Printf("Session for %s/%s has timed out\n", bucketName, objectName)

		if err := session.gcsClient.CancelUpload(context.Background(), session.uploadUrl); err != nil {
			fmt.Printf("Error cancelling upload session: %v\n", err)
		}

		_ = session.gcsClient.CancelUpload(ctx, session.uploadUrl)
		delete(*uploadSessions, sessionKey)
	}()
	return session, false
}

func handleWriteAt(ctx context.Context, conn net.Conn, session *UploadSession, header RequestHeader) error {
	var writeAtReq WriteAtRequestHeader
	if err := binary.Read(conn, binary.BigEndian, &writeAtReq); err != nil {
		return fmt.Errorf("failed to read WriteAtRequestHeader: %w", err)
	}
	if session.isAborted {
		return errors.New("upload session is aborted")
	}

	session.chunkLock.Lock()
	if session.currentChunkBeginOff != writeAtReq.ChunkBegin {
		if session.currentChunk != nil {
			select {
			case <-session.chunkWrittenEventChan:
			default:
				session.chunkLock.Unlock()
				fmt.Printf("out of odred write at, expected offset %d received %d!\n", session.currentChunkBeginOff, writeAtReq.ChunkBegin)
				return fmt.Errorf("out of order write at") // Need to retry later
			}
		}
	}
	if session.currentChunk == nil {
		session.currentChunk = utils.NewBuildableBuffer(uint32(writeAtReq.ChunkEnd - writeAtReq.ChunkBegin))
		session.chunkLock.Unlock()

		fmt.Printf("Write to buff, off: %d, size: %d\n", writeAtReq.Off, writeAtReq.Size)
		err := writeToChunkReader(writeAtReq, conn, session.currentChunk, session)
		if err != nil {
			return err
		}

		// fixme: now order of returns of write to buffer and load requests are not synchronized
		/* go */
		loadChunkToGCS(ctx, conn, header, writeAtReq, err, session)
		return nil
	}
	currentChunkReader := session.currentChunk
	session.chunkLock.Unlock()

	fmt.Printf("Write to buff, off: %d, size: %d\n", writeAtReq.Off, writeAtReq.Size)
	err := writeToChunkReader(writeAtReq, conn, currentChunkReader, session)
	if err != nil {
		return err
	}

	return nil
}

func loadChunkToGCS(ctx context.Context, conn net.Conn, header RequestHeader, writeAtReq WriteAtRequestHeader, err error, session *UploadSession) {
	chunkSize := writeAtReq.ChunkEnd - writeAtReq.ChunkBegin
	err = session.gcsClient.UploadObjectPart(ctx, session.uploadUrl, writeAtReq.ChunkBegin, session.currentChunk, chunkSize, writeAtReq.IsLast != 0)
	if err != nil {
		err := fmt.Errorf("failed to upload object part: %w", err)
		sendErrorResponse(conn, header.SequenceNumber, err)
		return
	}

	select {
	case session.chunkWrittenEventChan <- struct{}{}:
	default:
	}

	session.chunkLock.Lock()
	defer session.chunkLock.Unlock()
	session.currentChunk = nil
	session.currentChunkBeginOff = writeAtReq.ChunkEnd

	fmt.Printf("Chunk loaded [%d - %d] (%d bytes)\n",
		writeAtReq.Off, writeAtReq.Off+writeAtReq.Size, writeAtReq.Size)

	if writeAtReq.IsLast != 0 {
		session.isCompleted = true
		session.uploadEndTime = time.Now()
		session.cancelFunc()

		totalUploadTime := session.uploadEndTime.Sub(session.uploadStartTime)
		averageSpeed := float64(session.totalBytesUploaded) / totalUploadTime.Seconds()

		fmt.Printf("Upload completed for %s/%s\n", session.bucketName, session.objectName)
		fmt.Printf("Total uploaded: %d bytes in %.2f seconds (Average speed: %.2f MB/s)\n",
			session.totalBytesUploaded, totalUploadTime.Seconds(), averageSpeed/(1024*1024))
	}
	//sendSuccessResponse(conn, header.SequenceNumber, "CHUNK LOADED SUCCESSFULLY")
	return
}

func writeToChunkReader(writeAtReq WriteAtRequestHeader, conn net.Conn, currentChunkReader *utils.BuildableBuffer, session *UploadSession) error {
	if writeAtReq.Size <= 0 {
		return errors.New("invalid data size")
	}
	//startTime := time.Now()

	buf := make([]byte, writeAtReq.Size)
	if err := binary.Read(conn, binary.BigEndian, &buf); err != nil {
		return fmt.Errorf("failed to read WriteAtRequestHeader: %w", err)
	}
	if writeAtReq.Off-writeAtReq.ChunkBegin < 0 {
		panic("Lalala")
	}
	if err := currentChunkReader.WriteToOffset(uint32(writeAtReq.Off-writeAtReq.ChunkBegin), buf); err != nil {
		return err
	}

	//fillLoadingSpeedData(startTime, session, writeAtReq)
	return nil
}

func fillLoadingSpeedData(startTime time.Time, session *UploadSession, writeAtReq WriteAtRequestHeader) {
	elapsedTime := time.Since(startTime)
	session.totalBytesUploaded += writeAtReq.Size
	chunkSpeed := float64(writeAtReq.Size) / elapsedTime.Seconds()
	fmt.Printf("Uploaded chunk to GCS [%d - %d] (%d bytes) in %.2f seconds (%.2f MB/s)\n",
		writeAtReq.ChunkBegin, writeAtReq.ChunkEnd, writeAtReq.Size, elapsedTime.Seconds(), chunkSpeed/(1024*1024))
}

func handleGetResumeOffset(conn net.Conn, session *UploadSession, header RequestHeader) error {
	gcsOffset, complete, err := session.gcsClient.GetResumeOffset(session.sessionCtx, session.uploadUrl)
	if err != nil {
		return fmt.Errorf("failed to get resume offset from GCS: %w", err)
	}

	if complete {
		session.isCompleted = true
	}

	session.currentChunkBeginOff = gcsOffset

	if err := binary.Write(conn, binary.BigEndian, gcsOffset); err != nil {
		return fmt.Errorf("failed to send resume offset: %w", err)
	}

	return nil
}

func handleAbort(session *UploadSession) error {
	session.isAborted = true
	session.cancelFunc()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := session.gcsClient.CancelUpload(ctx, session.uploadUrl); err != nil {
		fmt.Printf("Error cancelling upload session: %v\n", err)
	}

	return nil
}

func sendSuccessResponse(conn net.Conn, seqNum uint32, message string) {
	fmt.Printf("Sending operation success response: %s\n", message)
	resp := ResponseHeader{
		SequenceNumber: seqNum,
		StatusCode:     0,
		MessageLength:  uint32(len(message)),
	}
	_ = binary.Write(conn, binary.BigEndian, &resp)
	_, _ = conn.Write([]byte(message))
}

func sendErrorResponse(conn net.Conn, seqNum uint32, err error) {
	if err == nil {
		sendSuccessResponse(conn, seqNum, "")
		return
	}

	msg := err.Error()
	resp := ResponseHeader{
		SequenceNumber: seqNum,
		StatusCode:     1,
		MessageLength:  uint32(len(msg)),
	}
	_ = binary.Write(conn, binary.BigEndian, &resp)
	_, _ = conn.Write([]byte(msg))
}
