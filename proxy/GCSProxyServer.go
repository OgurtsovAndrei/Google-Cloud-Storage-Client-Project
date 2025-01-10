package proxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
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
	connectionGroup, err := NewServerConnectionGroup(address, ctx, func(ctx context.Context, r interface{}, connections *ClientConnectionPool) error {
		return proxy.handleRequest(ctx, r, connections)
	})
	if err != nil {
		panic(err)
	}
	proxy.connectionGroup = connectionGroup
	return &proxy
}

func (proxyServer *GcsProxyServer) handleRequest(ctx context.Context, r interface{}, connections *ClientConnectionPool) error {
	switch req := r.(type) {
	case *InitUploadSessionRequest:
		log.Println("SERVER: Processing InitUploadSessionRequest:", req)
		err := handleInitUploadSession(ctx, &proxyServer.uploadSessions, &proxyServer.mutex, *req)
		if err != nil {
			resp := BuildErrorResponse(req.Header.RequestUid, err)
			connections.SendResponseMessage(ctx, resp)
			return err
		}
		resp := BuildSucceedResponse(req.Header.RequestUid, fmt.Sprintf("OK"))
		connections.SendResponseMessage(ctx, &resp)
		return nil
	case *GetResumeOffsetRequest:
		log.Println("SERVER: Processing GetResumeOffsetRequest:", req)
		off, err := handleGetResumeOffset(&proxyServer.uploadSessions, &proxyServer.mutex, req)
		if err != nil {
			resp := BuildErrorResponse(req.Header.RequestUid, err)
			connections.SendResponseMessage(ctx, resp)
			return err
		}
		resp := BuildSucceedResponse(req.Header.RequestUid, fmt.Sprintf("%d", off))
		connections.SendResponseMessage(ctx, &resp)
		return nil
	case *WriteAtRequest:
		log.Println("SERVER: Processing WriteAtRequest:", req)
		err := handleWriteAt(proxyServer.ctx, connections, &proxyServer.uploadSessions, &proxyServer.mutex, req)
		if err != nil {
			log.Println("SERVER: Error processing WriteAtRequest:", err)
			resp := BuildErrorResponse(req.Header.RequestUid, err)
			connections.SendResponseMessage(ctx, resp)
			return err
		}
		return nil
	case *AbortRequest:
		log.Println("SERVER: Processing AbortRequest:", req)
		err := handleAbort(&proxyServer.uploadSessions, &proxyServer.mutex, req)
		if err != nil {
			resp := BuildErrorResponse(req.Header.RequestUid, err)
			connections.SendResponseMessage(ctx, resp)
			return err
		}
		resp := BuildSucceedResponse(req.Header.RequestUid, fmt.Sprintf("OK"))
		connections.SendResponseMessage(ctx, &resp)
		return nil
	default:
		return &utils.Error{
			Code: utils.ErrCodeUnknownRequestType,
			Msg:  "Unknown request type",
			Tags: []string{utils.TagIllegalArgument},
		}
	}
}

func handleInitUploadSession(ctx context.Context, uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex, header InitUploadSessionRequest) error {
	sessionKey := header.Bucket + "/" + header.Object

	uploadSessionsMutex.Lock()
	defer uploadSessionsMutex.Unlock()

	session, failed := createNewSession(ctx, uploadSessionsMutex, header.Bucket, header.Object, uploadSessions, sessionKey)
	if failed {
		return &utils.Error{
			Code: utils.ErrCodeInitUploadSession,
			Msg:  fmt.Sprintf("Failed to create a new session for %s/%s", header.Bucket, header.Object),
			Tags: []string{utils.TagNetwork},
		}
	}
	(*uploadSessions)[sessionKey] = session

	return nil
}

func createNewSession(ctx context.Context, uploadSessionsMutex *sync.Mutex, bucketName, objectName string, uploadSessions *map[string]*UploadSession, sessionKey string) (session *UploadSession, failed bool) {
	sessionCtx, cancelFunc := context.WithTimeout(context.Background(), time.Hour)

	gcsClient, err := utils.NewGcsClient(sessionCtx)
	if err != nil {
		log.Printf("SERVER: Failed to create GCS client: %v\n", err)
		cancelFunc()
		return nil, true
	}

	uploadUrl, err := gcsClient.NewUploadSession(sessionCtx, bucketName, objectName)
	if err != nil {
		log.Printf("SERVER: Failed to create upload session: %v\n", err)
		cancelFunc()
		return nil, true
	}

	session = &UploadSession{
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

	onSessionFinishedGoroutine(ctx, uploadSessionsMutex, bucketName, objectName, uploadSessions, sessionKey, sessionCtx, session)
	return session, false
}

func onSessionFinishedGoroutine(ctx context.Context, uploadSessionsMutex *sync.Mutex, bucketName string, objectName string, uploadSessions *map[string]*UploadSession, sessionKey string, sessionCtx context.Context, session *UploadSession) {
	go func() {
		<-sessionCtx.Done()
		uploadSessionsMutex.Lock()
		defer uploadSessionsMutex.Unlock()

		if _, ok := (*uploadSessions)[sessionKey]; !ok {
			return
		}

		session.isAborted = true
		log.Printf("SERVER: Session for %s/%s has timed out\n", bucketName, objectName)

		if err := session.gcsClient.CancelUpload(context.Background(), session.uploadUrl); err != nil {
			log.Printf("SERVER: Error cancelling upload session: %v\n", err)
		}

		_ = session.gcsClient.CancelUpload(ctx, session.uploadUrl)
		delete(*uploadSessions, sessionKey)
	}()
}

func handleWriteAt(ctx context.Context, connections *ClientConnectionPool, uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex, header *WriteAtRequest) error {
	session, err := getSession(uploadSessions, uploadSessionsMutex, header)
	if err != nil {
		return err
	}

	if err := validateSessionState(session, header); err != nil {
		return err
	}

	if session.currentChunk == nil {
		return handleNewChunk(ctx, connections, session, header)
	}

	return handleExistingChunk(session, header)
}

func getSession(uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex, header *WriteAtRequest) (*UploadSession, error) {
	sessionKey := header.Bucket + "/" + header.Object

	uploadSessionsMutex.Lock()
	session, exists := (*uploadSessions)[sessionKey]
	uploadSessionsMutex.Unlock()

	if !exists {
		return nil, &utils.Error{
			Code: utils.ErrCodeNotFound,
			Msg:  fmt.Sprintf("SERVER: Upload session for %s/%s not found", header.Bucket, header.Object),
			Tags: []string{utils.TagNotFound},
		}
	}
	return session, nil
}

func validateSessionState(session *UploadSession, header *WriteAtRequest) error {
	if session.isAborted {
		return &utils.Error{
			Code: utils.ErrCodeAbortFailed,
			Msg:  "SERVER: Upload session is aborted",
			Tags: []string{utils.TagIllegalArgument},
		}
	}

	if header.WriteAtHeader.ChunkBegin != session.resumeOffset {
		return &utils.Error{
			Code: utils.ErrCodeOutOfOrderWrite,
			Msg:  fmt.Sprintf("SERVER: Chunk begin %d does not match resume offset %d", header.WriteAtHeader.ChunkBegin, session.resumeOffset),
			Tags: []string{utils.TagOutOfOrder},
		}
	}
	return nil
}

func handleNewChunk(ctx context.Context, connections *ClientConnectionPool, session *UploadSession, header *WriteAtRequest) error {
	session.chunkLock.Lock()
	defer session.chunkLock.Unlock()

	session.currentChunk = utils.NewBuildableBuffer(uint32(header.WriteAtHeader.ChunkEnd - header.WriteAtHeader.ChunkBegin))
	session.currentChunkBeginOff = header.WriteAtHeader.ChunkBegin
	session.currentChunkEndOff = header.WriteAtHeader.ChunkEnd

	err := writeToChunkReader(
		uint32(header.WriteAtHeader.Size),
		uint32(header.WriteAtHeader.Off-header.WriteAtHeader.ChunkBegin),
		header.Data,
		session.currentChunk,
	)
	if err != nil {
		return &utils.Error{
			Code:  utils.ErrCodeWriteAtFailed,
			Msg:   "SERVER: Failed to write to chunk reader",
			Cause: err,
			Tags:  []string{utils.TagLogOnly},
		}
	}

	go func() {
		err := loadChunkToGcdGoroutine(ctx, session, header.WriteAtHeader.IsLast != 0)
		if err != nil {
			customErr := &utils.Error{
				Code:  utils.ErrCodeUploadChunkFailed,
				Msg:   "SERVER: Failed to upload chunk",
				Cause: err,
				Tags:  []string{utils.TagNetwork, utils.TagRetryable},
			}
			resp := BuildErrorResponse(header.Header.RequestUid, customErr)
			connections.SendResponseMessage(ctx, resp)
			return
		}
		resp := BuildSucceedResponse(header.Header.RequestUid, fmt.Sprintf("OK"))
		connections.SendResponseMessage(ctx, &resp)
	}()

	return nil
}

func handleExistingChunk(session *UploadSession, header *WriteAtRequest) error {
	session.chunkLock.Lock()
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
		return &utils.Error{
			Code:  utils.ErrCodeWriteAtFailed,
			Msg:   "SERVER: Failed to write to chunk reader",
			Cause: err,
			Tags:  []string{utils.TagLogOnly},
		}
	}
	return nil
}

func loadChunkToGcdGoroutine(ctx context.Context, session *UploadSession, IsLast bool) error {
	chunkSize := session.currentChunkEndOff - session.currentChunkBeginOff
	if chunkSize <= 0 {
		panic("SERVER: invalid chunk size") // inner process forget to set value
	}
	err := session.gcsClient.UploadObjectPart(ctx, session.uploadUrl, session.currentChunkBeginOff, session.currentChunk, chunkSize, IsLast)
	if err != nil {
		customErr := &utils.Error{
			Code:  utils.ErrCodeUploadChunkFailed,
			Msg:   "SERVER: failed to upload object part",
			Cause: err,
			Tags:  []string{utils.TagNetwork, utils.TagRetryable},
		}
		return customErr
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

		log.Printf("SERVER: Upload completed for %s/%s\n", session.bucketName, session.objectName)
		log.Printf("SERVER: Total uploaded: %d bytes in %.2f seconds (Average speed: %.2f MB/s)\n",
			session.totalBytesUploaded, totalUploadTime.Seconds(), averageSpeed/(1024*1024))
	}
	return nil
}

func writeToChunkReader(size uint32, offsetInChunkReader uint32, reader io.Reader, currentChunkReader *utils.BuildableBuffer) error {
	if size <= 0 {
		return errors.New("SERVER: invalid Data size")
	}
	buf := make([]byte, size)
	if err := binary.Read(reader, binary.BigEndian, &buf); err != nil {
		return fmt.Errorf("SERVER: failed to read WriteAtRequestHeader: %w", err)
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
		return -1, fmt.Errorf("SERVER: upload session for %s/%s not found", header.Bucket, header.Object)
	}

	gcsOffset, complete, err := session.gcsClient.GetResumeOffset(session.sessionCtx, session.uploadUrl)
	if err != nil {
		return -1, fmt.Errorf("SERVER: failed to get resume offset from GCS: %w", err)
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
		return fmt.Errorf("SERVER: upload session for %s/%s not found", header.Bucket, header.Object)
	}

	session.isAborted = true
	session.cancelFunc()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := session.gcsClient.CancelUpload(ctx, session.uploadUrl); err != nil {
		log.Printf("SERVER: Error cancelling upload session: %v\n", err)
	}

	return nil
}
