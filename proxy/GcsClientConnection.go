package proxy

import (
	"awesomeProject/netUtils"
	"awesomeProject/utils"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
)

var (
	checkCancelConnThreshold int64 = 1 * 1024 * 1024
)

type ClientConnectionGroup struct {
	clientID         string
	messages         chan *RequestMessage
	responseMap      map[uint32]chan *ResponseMessage
	responseMapMutex sync.Mutex
	address          string
	ctx              context.Context
	nConnections     int
	uid              uint32
}

func (cg *ClientConnectionGroup) NextUid() uint32 {
	return atomic.AddUint32(&cg.uid, 1)
}

func generateRandomClientID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func NewClientConnectionGroup(bufferSize int, address string, ctx context.Context, nConnections int) *ClientConnectionGroup {
	clientID := generateRandomClientID()
	log.Printf("CLIENT: Creating ClientConnectionGroup with clientID=%s, bufferSize=%d, address=%s, nConnections=%d",
		clientID, bufferSize, address, nConnections)

	cg := &ClientConnectionGroup{
		clientID:     clientID,
		messages:     make(chan *RequestMessage, bufferSize),
		responseMap:  make(map[uint32]chan *ResponseMessage),
		address:      address,
		ctx:          ctx,
		nConnections: nConnections,
	}

	for i := 0; i < nConnections; i++ {
		goHandleClientConnection(cg, i)
	}

	return cg
}

func goHandleClientConnection(cg *ClientConnectionGroup, i int) {
	go func(i int) {
		log.Printf("CLIENT: Starting connection goroutine %d", i)
		err := cg.handleConnection(i)
		log.Printf("CLIENT: Connection goroutine %d error: %v", i, err)
	}(i)
}

func (cg *ClientConnectionGroup) createConnection() (net.Conn, error) {
	log.Printf("CLIENT: Creating connection to %s", cg.address)
	//return net.Dial("tcp", cg.address)
	return netUtils.NewUnstableDialer(checkCancelConnThreshold).Dial("tcp", cg.address)
}

func (cg *ClientConnectionGroup) handleConnection(i int) error {
	log.Println("CLIENT: Handling connection")
	conn, err := cg.createConnection()
	if err != nil {
		log.Printf("CLIENT: handleConnection: Error creating connection: %v", err)
		return err
	}
	defer conn.Close()

	if err := cg.sendHandshake(conn); err != nil {
		log.Printf("CLIENT: handleConnection: handshake failed: %v", err)
		return err
	}
	log.Printf("CLIENT: Connection established for goroutine %d", i)

	readErrCh := make(chan error, 1)
	writeErrCh := make(chan error, 1)

	go cg.readFromConnGoroutine(conn, readErrCh)
	go cg.writeToConnGoroutine(conn, writeErrCh)

	select {
	case <-cg.ctx.Done():
		log.Println("CLIENT: handleConnection: Context canceled")
		return utils.CastContextError(cg.ctx.Err())
	case err = <-readErrCh:
		log.Printf("CLIENT: handleConnection: Read error: %v", err)
	case err = <-writeErrCh:
		log.Printf("CLIENT: handleConnection: Write error: %v", err)
	}

	log.Println("CLIENT: handleConnection: Error received, reopening connection...", err)
	var customErr *utils.Error
	if errors.As(err, &customErr) {
		if !customErr.HasTag(utils.TagContextCanceled) &&
			(customErr.HasTag(utils.TagNetwork) || customErr.HasTag(utils.TagRetryable)) {
			goHandleClientConnection(cg, i)
		}
	} else {
		log.Printf("CLIENT: handleConnection: Failed to cast error, connection %d will not be reopened: %v", i, err)
	}
	return err
}

func (cg *ClientConnectionGroup) sendHandshake(conn net.Conn) error {
	handshakeReq := HandshakeRequest{
		Header: RequestHeader{
			RequestUid:  cg.NextUid(),
			RequestType: MessageTypeHandshake,
		},
		HandshakeHeader: HandshakeHeader{
			ClientIDLength: uint32(len(cg.clientID)),
		},
		ClientID: cg.clientID,
	}

	message := handshakeReq.ToRequestMessage()
	if _, err := io.Copy(conn, NewRequestReader(&message)); err != nil {
		return err
	}

	resp, err := ReadResponse(conn)
	if err != nil {
		return err
	}
	if resp.IsErr() {
		return fmt.Errorf("CLIENT: handshake (%d) error from server: %s", handshakeReq.Header.RequestUid, resp.ToReadableString())
	}

	log.Printf("CLIENT: sendHandshake: handshake success, server responded: %s", resp.Data)
	return nil
}

func (cg *ClientConnectionGroup) writeToConnGoroutine(conn net.Conn, writeErrCh chan<- error) {
	log.Println("f started")
	defer log.Println("CLIENT: writeToConnGoroutine: Goroutine finished")
	for {
		select {
		case req := <-cg.messages:
			log.Printf("CLIENT: writeToConnGoroutine: Sending request for RequestUid=%d", req.Header.RequestUid)
			if _, err := io.Copy(conn, NewRequestReader(req)); err != nil {
				log.Printf("CLIENT: writeToConnGoroutine: Error writing request %s: %v", req.Header.ToReadableString(), err)

				tags := []string{
					utils.TagNetwork,
					utils.TagConnectionDown,
					utils.TagRetryable,
				}

				myErr := utils.Error{
					Code:  utils.ErrCodeHandleConnectionFailed,
					Msg:   fmt.Sprintf("CLIENT: writeToConnGoroutine: Error writing request for RequestUid=%d of type %s", req.Header.RequestUid, getTypeReadableName(req.Header.RequestType)),
					Cause: err,
					Tags:  tags,
				}

				writeErrCh <- &myErr
				message, err := ErrorToResponseMessage(req.Header.RequestUid, &myErr)
				if err != nil {
					log.Printf("CLIENT: writeToConnGoroutine: Error writing response: %v", err)
				}
				cg.DispatchResponse(message)
				return
			}
			log.Printf("CLIENT: writeToConnGoroutine: Request sent successfully for RequestUid=%d", req.Header.RequestUid)
		case <-cg.ctx.Done():
			log.Println("CLIENT: writeToConnGoroutine: Context canceled")
			writeErrCh <- utils.CastContextError(cg.ctx.Err())
			return
		}
	}
}

func (cg *ClientConnectionGroup) readFromConnGoroutine(conn net.Conn, readErrCh chan<- error) {
	for {
		select {
		case <-cg.ctx.Done():
			log.Println("CLIENT: readFromConnGoroutine: Context canceled")
			readErrCh <- utils.CastContextError(cg.ctx.Err())
			return
		default:
			resp, err := ReadResponse(conn)
			log.Printf("CLIENT: readFromConnGoroutine: Received response: %+v\n", resp)
			if err != nil {
				log.Printf("CLIENT: readFromConnGoroutine: Error reading Data: %v", err)
				readErrCh <- err
				return
			}
			cg.DispatchResponse(resp)
		}
	}
}

// DispatchResponse dispatchResponse routes a response to the appropriate channel.
func (cg *ClientConnectionGroup) DispatchResponse(resp *ResponseMessage) {

	log.Printf("CLIENT: DispatchResponse called with response: %+v", resp)
	if resp.IsErr() {
		customErr, convErr := resp.AsErr()
		if convErr == nil {
			if customErr.HasTag(utils.TagLogOnly) {
				log.Printf("CLIENT: Received error response: %s", customErr.Msg)
				return
			}
		}
	}

	cg.responseMapMutex.Lock()
	defer cg.responseMapMutex.Unlock()

	ch, exists := cg.responseMap[resp.Header.RequestUid]
	if !exists {
		log.Printf("CLIENT: Response channel for RequestUid=%d not found, response: %+v", resp.Header.RequestUid, resp)
		if len(cg.responseMap) == 0 {
			log.Println("CLIENT: responseMap is empty.")
		}
		for key := range cg.responseMap {
			log.Printf("CLIENT: Available response channel for RequestUid=%d", key)
		}
		return
	}

	ch <- resp
	close(ch)
	delete(cg.responseMap, resp.Header.RequestUid)
	log.Printf("CLIENT: Response dispatched for RequestUid=%d, response: %+v", resp.Header.RequestUid, resp)
}

func (cg *ClientConnectionGroup) SendMessage(ctx context.Context, msg *RequestMessage) *utils.Error {
	cg.responseMapMutex.Lock()
	if _, exists := cg.responseMap[msg.Header.RequestUid]; !exists {
		log.Printf("CLIENT: Response channel not found for given request ID %d", msg.Header.RequestUid)
	}
	cg.responseMapMutex.Unlock()
	select {
	case cg.messages <- msg:
		return nil
	case <-ctx.Done():
		return nil // ignore context cancellation, as error is already reported
	}
}

func (cg *ClientConnectionGroup) CreateRespondChannel(RequestUid uint32) {
	cg.responseMapMutex.Lock()
	if _, exists := cg.responseMap[RequestUid]; !exists {
		cg.responseMap[RequestUid] = make(chan *ResponseMessage, 1)
	}
	cg.responseMapMutex.Unlock()
}

func (cg *ClientConnectionGroup) WaitResponse(ctx context.Context, requestUid uint32, RequestType uint32) (*ResponseMessage, *utils.Error) {
	cg.responseMapMutex.Lock()
	ch, exists := cg.responseMap[requestUid]
	cg.responseMapMutex.Unlock()
	if !exists {
		return nil, &utils.Error{
			Code: utils.ErrCodeResponseChannelNotFound,
			Msg:  fmt.Sprintf("CLIENT: Response channel not found for given request ID %d", requestUid),
			Tags: []string{utils.TagIllegalArgument},
		}
	}
	log.Println("CLIENT: Waiting for response... for request - " + fmt.Sprint(requestUid) + " of type - " + fmt.Sprint(RequestType))
	select {
	case <-ctx.Done():
		return nil, utils.CastContextError(ctx.Err())
	case resp := <-ch:
		return resp, nil
	}
}
