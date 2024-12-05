package main

import (
	"awesomeProject/utils"
	"awesomeProject/writers"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

type UploadSession struct {
	uploadUrl         string
	gcsClient         *utils.GcsClient
	resumeOffset      int64
	isAborted         bool
	isCompleted       bool
	activeConnections int
	bucketName        string
	objectName        string
	sessionCtx        context.Context
	cancelFunc        context.CancelFunc
}

func startGCSProxyServer(ctx context.Context, listenAddress string) error {
	var (
		uploadSessions      = make(map[string]*UploadSession)
		uploadSessionsMutex sync.Mutex
	)

	listener, err := net.Listen("tcp", listenAddress)
	if err != nil {
		return fmt.Errorf("error starting server: %w", err)
	}
	fmt.Printf("GCSProxyServer listening on %s\n", listenAddress)

	go func() {
		<-ctx.Done()
		listener.Close()
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
		go handleConnection(ctx, conn, &uploadSessions, &uploadSessionsMutex)
	}
}

func handleConnection(ctx context.Context, conn net.Conn, uploadSessions *map[string]*UploadSession, uploadSessionsMutex *sync.Mutex) {
	defer conn.Close()

	var header writers.RequestHeader
	if err := binary.Read(conn, binary.BigEndian, &header); err != nil {
		fmt.Printf("Error reading request header: %v\n", err)
		return
	}

	if header.RequestType != writers.MessageTypeInitConnection {
		fmt.Printf("First request must be an init connection request\n")
		return
	}

	var initReq writers.InitConnectionRequestHeader
	if err := binary.Read(conn, binary.BigEndian, &initReq); err != nil {
		fmt.Printf("Failed to read InitConnectionRequestHeader: %v\n", err)
		return
	}

	bucketNameBytes := make([]byte, initReq.BucketNameLength)
	if _, err := io.ReadFull(conn, bucketNameBytes); err != nil {
		fmt.Printf("Failed to read bucket name: %v\n", err)
		return
	}
	bucketName := string(bucketNameBytes)

	objectNameBytes := make([]byte, initReq.ObjectNameLength)
	if _, err := io.ReadFull(conn, objectNameBytes); err != nil {
		fmt.Printf("Failed to read object name: %v\n", err)
		return
	}
	objectName := string(objectNameBytes)

	sessionKey := bucketName + "/" + objectName
	uploadSessionsMutex.Lock()
	session, exists := (*uploadSessions)[sessionKey]
	if !exists {
		var failed bool
		session, failed = createNewSession(ctx, uploadSessionsMutex, bucketName, objectName, session, uploadSessions, sessionKey)
		if failed {
			return
		}
	} else {
		session.activeConnections++
	}
	uploadSessionsMutex.Unlock()

	for {
		var header writers.RequestHeader
		if err := binary.Read(conn, binary.BigEndian, &header); err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading request header: %v\n", err)
			return
		}

		switch header.RequestType {
		case writers.MessageTypeUploadPart:
			if err := handleWriteAt(session.sessionCtx, conn, session); err != nil {
				fmt.Printf("Error handling upload part: %v\n", err)
				return
			}
		case writers.MessageTypeGetResumeOffset:
			if err := handleGetResumeOffset(conn, session); err != nil {
				fmt.Printf("Error handling get resume offset: %v\n", err)
				return
			}
		case writers.MessageTypeAbort:
			if err := handleAbort(session); err != nil {
				fmt.Printf("Error handling abort: %v\n", err)
				return
			}
		default:
			fmt.Printf("Unknown request type: %d\n", header.RequestType)
			return
		}
	}

	uploadSessionsMutex.Lock()
	session.activeConnections--
	if session.activeConnections == 0 && (session.isAborted || session.isCompleted) {
		session.cancelFunc()
		session.gcsClient.CancelUpload(ctx, session.uploadUrl)
		delete(*uploadSessions, sessionKey)
	}
	uploadSessionsMutex.Unlock()
}

func createNewSession(ctx context.Context, uploadSessionsMutex *sync.Mutex, bucketName string, objectName string, session *UploadSession, uploadSessions *map[string]*UploadSession, sessionKey string) (*UploadSession, bool) {
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
	session = &UploadSession{
		uploadUrl:         uploadUrl,
		gcsClient:         gcsClient,
		resumeOffset:      0,
		isAborted:         false,
		isCompleted:       false,
		activeConnections: 1,
		bucketName:        bucketName,
		objectName:        objectName,
		sessionCtx:        sessionCtx,
		cancelFunc:        cancelFunc,
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

func handleWriteAt(ctx context.Context, conn net.Conn, session *UploadSession) error {
	var writeAtReq writers.WriteAtRequestHeader
	if err := binary.Read(conn, binary.BigEndian, &writeAtReq); err != nil {
		return fmt.Errorf("failed to read WriteAtRequestHeader: %w", err)
	}

	dataSize := writeAtReq.ChunkEnd - writeAtReq.ChunkBegin
	if dataSize <= 0 {
		return errors.New("invalid data size")
	}

	if session.isAborted {
		return errors.New("upload session is aborted")
	}
	if writeAtReq.ChunkBegin != session.resumeOffset {
		return fmt.Errorf("chunk begin %d does not match resume offset %d", writeAtReq.ChunkBegin, session.resumeOffset)
	}

	limitedReader := io.LimitReader(conn, dataSize)
	err := session.gcsClient.UploadObjectPart(ctx, session.uploadUrl, writeAtReq.ChunkBegin, limitedReader, dataSize, writeAtReq.IsLast != 0)
	if err != nil {
		return fmt.Errorf("failed to upload object part: %w", err)
	}

	session.resumeOffset = writeAtReq.ChunkEnd

	if writeAtReq.IsLast != 0 {
		session.isCompleted = true
		session.cancelFunc()
	}

	return nil
}

func handleGetResumeOffset(conn net.Conn, session *UploadSession) error {
	resumeOffset := session.resumeOffset

	if err := binary.Write(conn, binary.BigEndian, resumeOffset); err != nil {
		return fmt.Errorf("failed to send resume offset: %w", err)
	}

	return nil
}

func handleAbort(session *UploadSession) error {
	session.isAborted = true
	session.cancelFunc()
	if err := session.gcsClient.CancelUpload(context.Background(), session.uploadUrl); err != nil {
		fmt.Printf("Error cancelling upload session: %v\n", err)
	}
	return nil
}
