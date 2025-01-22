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
	resumeOffset       int64
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

	chunkLock            sync.Mutex
	currentChunkBeginOff int64
	currentChunkEndOff   int64
	currentChunk         *utils.BuildableBuffer
}

type GcsProxyServer struct {
	connectionGroup *ServerConnectionGroup
	uploadSessions  map[string]*UploadSession
	mutex           sync.Mutex
	ctx             context.Context
}

func NewGcsProxyServer(ctx context.Context, address string) *GcsProxyServer {
	proxy := GcsProxyServer{
		uploadSessions: make(map[string]*UploadSession),
		ctx:            ctx,
	}
	connectionGroup, err := NewServerConnectionGroup(address, ctx, func(ctx context.Context, r interface{}, conn net.Conn) error {
		return proxy.handleRequest(ctx, r, conn)
	})
	if err != nil {
		panic(err)
	}
	proxy.connectionGroup = connectionGroup
	return &proxy
}

func (proxyServer *GcsProxyServer) handleRequest(ctx context.Context, r interface{}, respondConn io.Writer) error {
	switch req := r.(type) {
	case *InitUploadSessionRequest:
		fmt.Println("Processing InitUploadSessionRequest:", req)
		err := handleInitConnection(ctx, &proxyServer.uploadSessions, &proxyServer.mutex, *req)
		if err != nil {
			SendErrorResponse(respondConn, req.Header.RequestUid, err)
			return err
		}
		SendSuccessResponse(respondConn, req.Header.RequestUid, fmt.Sprintf("OK"))
		return nil
	case *GetResumeOffsetRequest:
		fmt.Println("Processing GetResumeOffsetRequest:", req)
		off, err := handleGetResumeOffset(&proxyServer.uploadSessions, &proxyServer.mutex, req)
		if err != nil {
			SendErrorResponse(respondConn, req.Header.RequestUid, err)
			return err
		}
		SendSuccessResponse(respondConn, req.Header.RequestUid, fmt.Sprintf("%d", off))
		return nil
	case *WriteAtRequest:
		fmt.Println("Processing WriteAtRequest:", req)
		err := handleWriteAt(proxyServer.ctx, respondConn, &proxyServer.uploadSessions, &proxyServer.mutex, req)
		if err != nil {
			SendErrorResponse(respondConn, req.Header.RequestUid, err)
			return err
		}
		return nil
	case *AbortRequest:
		fmt.Println("Processing AbortRequest:", req)
		err := handleAbort(&proxyServer.uploadSessions, &proxyServer.mutex, req)
		if err != nil {
			SendErrorResponse(respondConn, req.Header.RequestUid, err)
			return err
		}
		SendSuccessResponse(respondConn, req.Header.RequestUid, fmt.Sprintf("OK"))
		return nil
	default:
		return errors.New("unknown request type")
	}
}

func handleInitConnection(ctx context.Context, uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex, header InitUploadSessionRequest) error {
	sessionKey := header.Bucket + "/" + header.Object

	uploadSessionsMutex.Lock()
	defer uploadSessionsMutex.Unlock()

	session, failed := createNewSession(ctx, uploadSessionsMutex, header.Bucket, header.Object, uploadSessions, sessionKey)
	if failed {
		return fmt.Errorf("failed to create a new session for %s/%s", header.Bucket, header.Object)
	}
	(*uploadSessions)[sessionKey] = session

	return nil
}

func createNewSession(ctx context.Context, uploadSessionsMutex *sync.Mutex, bucketName string, objectName string, uploadSessions *map[string]*UploadSession, sessionKey string) (*UploadSession, bool) {
	sessionCtx, cancelFunc := context.WithTimeout(context.Background(), time.Hour)

	gcsClient, err := utils.NewGcsClient(sessionCtx)
	if err != nil {
		fmt.Printf("Failed to create GCS client: %v\n", err)
		cancelFunc()
		return nil, true
	}
	uploadUrl, err := gcsClient.NewUploadSession(sessionCtx, bucketName, objectName)
	if err != nil {
		fmt.Printf("Failed to create upload session: %v\n", err)
		cancelFunc()
		return nil, true
	}
	session := &UploadSession{
		uploadUrl:         uploadUrl,
		gcsClient:         gcsClient,
		resumeOffset:      0,
		isAborted:         false,
		isCompleted:       false,
		activeConnections: 0,
		bucketName:        bucketName,
		objectName:        objectName,
		sessionCtx:        sessionCtx,
		cancelFunc:        cancelFunc,
		uploadStartTime:   time.Now(),
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

func handleWriteAt(ctx context.Context, respondConn io.Writer, uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex, header *WriteAtRequest) (error error) {
	sessionKey := header.Bucket + "/" + header.Object

	uploadSessionsMutex.Lock()
	session, exists := (*uploadSessions)[sessionKey]
	uploadSessionsMutex.Unlock()

	if !exists {
		return fmt.Errorf("upload session for %s/%s not found", header.Bucket, header.Object)
	}

	if session.isAborted {
		return errors.New("upload session is aborted")
	}
	if header.WriteAtHeader.ChunkBegin != session.resumeOffset {
		return fmt.Errorf("chunk begin %d does not match resume offset %d", header.WriteAtHeader.ChunkBegin, session.resumeOffset)
	}

	session.chunkLock.Lock()
	if session.currentChunkBeginOff != header.WriteAtHeader.ChunkBegin {
		if session.currentChunk != nil {
			session.chunkLock.Unlock()
			fmt.Printf("out of odred write at, expected offset %d received %d!\n", session.currentChunkBeginOff, header.WriteAtHeader.ChunkBegin)
			return fmt.Errorf("out of order write at")
		}
	}
	if session.currentChunk == nil {
		session.currentChunk = utils.NewBuildableBuffer(uint32(header.WriteAtHeader.ChunkEnd - header.WriteAtHeader.ChunkBegin))
		session.currentChunkBeginOff = header.WriteAtHeader.ChunkBegin
		session.currentChunkEndOff = header.WriteAtHeader.ChunkEnd
		session.chunkLock.Unlock()

		fmt.Printf("Write to buff, off: %d, size: %d\n", header.WriteAtHeader.Off, header.WriteAtHeader.Size)
		err := writeToChunkReader(
			uint32(header.WriteAtHeader.Size),
			uint32(header.WriteAtHeader.Off-header.WriteAtHeader.ChunkBegin),
			header.Data,
			session.currentChunk,
		)
		if err != nil {
			return err
		}

		// fixme: now order of returns of write to buffer and load requests are not synchronized

		go func() {
			err := loadChunkToGcdGoroutine(ctx, session, header.WriteAtHeader.IsLast != 0)
			if err != nil {
				// todo: signal failed write
				SendErrorResponse(respondConn, header.Header.RequestUid, err)
			}
			// todo: signal succeed write
			SendSuccessResponse(respondConn, header.Header.RequestUid, "OK")
		}()
		return nil
	}
	currentChunkReader := session.currentChunk
	session.chunkLock.Unlock()

	limitedReader := io.LimitReader(header.Data, header.WriteAtHeader.Size)

	err := writeToChunkReader(
		uint32(header.WriteAtHeader.Size),
		uint32(header.WriteAtHeader.Off-header.WriteAtHeader.ChunkBegin),
		limitedReader,
		currentChunkReader,
	)
	if err != nil {
		return err
	}

	return nil
}

func loadChunkToGcdGoroutine(ctx context.Context, session *UploadSession, IsLast bool) error {
	chunkSize := session.currentChunkEndOff - session.currentChunkBeginOff
	if chunkSize <= 0 {
		panic("invalid chunk size") // inner process forget to set value
	}
	err := session.gcsClient.UploadObjectPart(ctx, session.uploadUrl, session.currentChunkBeginOff, session.currentChunk, chunkSize, IsLast)
	if err != nil {
		err := fmt.Errorf("failed to upload object part: %w", err)
		return err
	}

	session.chunkLock.Lock()
	defer session.chunkLock.Unlock()
	session.currentChunk = nil
	session.currentChunkBeginOff = session.currentChunkEndOff
	session.resumeOffset = session.currentChunkBeginOff

	if IsLast {
		session.isCompleted = true
		session.uploadEndTime = time.Now()
		session.cancelFunc()

		totalUploadTime := session.uploadEndTime.Sub(session.uploadStartTime)
		averageSpeed := float64(session.totalBytesUploaded) / totalUploadTime.Seconds()

		fmt.Printf("Upload completed for %s/%s\n", session.bucketName, session.objectName)
		fmt.Printf("Total uploaded: %d bytes in %.2f seconds (Average speed: %.2f MB/s)\n",
			session.totalBytesUploaded, totalUploadTime.Seconds(), averageSpeed/(1024*1024))
	}
	return nil
}

func writeToChunkReader(size uint32, offsetInChunkReader uint32, reader io.Reader, currentChunkReader *utils.BuildableBuffer) error {
	if size <= 0 {
		return errors.New("invalid Data size")
	}
	buf := make([]byte, size)
	if err := binary.Read(reader, binary.BigEndian, &buf); err != nil {
		return fmt.Errorf("failed to read WriteAtRequestHeader: %w", err)
	}
	if err := currentChunkReader.WriteToOffset(offsetInChunkReader, buf); err != nil {
		return err
	}
	return nil
}

func handleGetResumeOffset(uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex, header *GetResumeOffsetRequest) (int64, error) {
	sessionKey := header.Bucket + "/" + header.Object

	uploadSessionsMutex.Lock()
	session, exists := (*uploadSessions)[sessionKey]
	uploadSessionsMutex.Unlock()

	if !exists {
		return -1, fmt.Errorf("upload session for %s/%s not found", header.Bucket, header.Object)
	}

	gcsOffset, complete, err := session.gcsClient.GetResumeOffset(session.sessionCtx, session.uploadUrl)
	if err != nil {
		return -1, fmt.Errorf("failed to get resume offset from GCS: %w", err)
	}

	if complete {
		session.isCompleted = true
	}

	session.resumeOffset = gcsOffset
	return gcsOffset, nil
}

func handleAbort(uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex, header *AbortRequest) error {
	sessionKey := header.Bucket + "/" + header.Object

	uploadSessionsMutex.Lock()
	session, exists := (*uploadSessions)[sessionKey]
	uploadSessionsMutex.Unlock()

	if !exists {
		return fmt.Errorf("upload session for %s/%s not found", header.Bucket, header.Object)
	}

	session.isAborted = true
	session.cancelFunc()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := session.gcsClient.CancelUpload(ctx, session.uploadUrl); err != nil {
		fmt.Printf("Error cancelling upload session: %v\n", err)
	}

	return nil
}
