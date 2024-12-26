package proxy

import (
	"awesomeProject/utils"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"sync"
)

type ClientConnectionGroup struct {
	messages         chan *RequestMessage
	responseMap      map[uint32]chan *ResponseMessage
	responseMapMutex sync.Mutex
	address          string
	ctx              context.Context
	nConnections     int
}

func NewClientConnectionGroup(bufferSize int, address string, ctx context.Context, nConnections int) *ClientConnectionGroup {
	log.Printf("Creating ClientConnectionGroup with bufferSize=%d, address=%s, nConnections=%d", bufferSize, address, nConnections)
	cg := &ClientConnectionGroup{
		messages:     make(chan *RequestMessage, bufferSize),
		responseMap:  make(map[uint32]chan *ResponseMessage),
		address:      address,
		ctx:          ctx,
		nConnections: nConnections,
	}

	for i := 0; i < nConnections; i++ {
		goHandleConnection(cg, i)
	}

	return cg
}

func goHandleConnection(cg *ClientConnectionGroup, i int) {
	go func(i int) {
		log.Printf("Starting connection goroutine %d", i)
		err := cg.handleConnection(i)
		log.Printf("Connection goroutine %d error: %v", i, err)
	}(i)
}

func (cg *ClientConnectionGroup) createConnection() (net.Conn, error) {
	log.Printf("Creating connection to %s", cg.address)
	return net.Dial("tcp", cg.address)
}

func (cg *ClientConnectionGroup) handleConnection(i int) error {
	log.Println("Handling connection")
	conn, err := cg.createConnection()
	if err != nil {
		log.Printf("handleConnection: Error creating connection: %v", err)
		return err
	}
	defer conn.Close()

	readErrCh := make(chan error, 1)
	writeErrCh := make(chan error, 1)

	go cg.readFromConnGoroutine(conn, readErrCh)
	go cg.writeToConnGoroutine(conn, writeErrCh)

	select {
	case <-cg.ctx.Done():
		log.Println("handleConnection: Context canceled")
		return utils.CastContextError(cg.ctx.Err())
	case err := <-readErrCh:
		log.Printf("handleConnection: Read error: %v", err)
	case err := <-writeErrCh:
		log.Printf("handleConnection: Write error: %v", err)
	}

	var customErr *utils.Error
	if errors.As(err, &customErr) {
		if !customErr.HasTag(utils.TagContextCanceled) &&
			(customErr.HasTag(utils.TagNetwork) || customErr.HasTag(utils.TagRetryable)) {
			goHandleConnection(cg, i)
		}
	}
	return err
}

func (cg *ClientConnectionGroup) writeToConnGoroutine(conn net.Conn, writeErrCh chan<- error) {
	log.Println("Starting writeToConnGoroutine")
	for {
		select {
		case req := <-cg.messages:
			log.Printf("writeToConnGoroutine: Sending request for RequestUid=%d", req.Header.RequestUid)
			if _, err := io.Copy(conn, NewRequestReader(req)); err != nil {
				log.Printf("writeToConnGoroutine: Error writing request: %v", err)
				writeErrCh <- err

				tags := []string{
					utils.TagNetwork,
					utils.TagConnectionDown,
					utils.TagRetryable,
				}

				myErr := utils.Error{
					Code:  utils.ErrCodeHandleConnectionFailed,
					Msg:   "writeToConnGoroutine: Error writing request",
					Cause: err,
					Tags:  tags,
				}
				message, err := ErrorToResponseMessage(req.Header.RequestUid, &myErr)
				if err != nil {
					log.Printf("writeToConnGoroutine: Error writing response: %v", err)
				}
				cg.dispatchResponse(message)
				return
			}
		case <-cg.ctx.Done():
			log.Println("writeToConnGoroutine: Context canceled")
			writeErrCh <- utils.CastContextError(cg.ctx.Err())
			return
		}
	}
}

func (cg *ClientConnectionGroup) readFromConnGoroutine(conn net.Conn, readErrCh chan<- error) {
	log.Println("c")
	for {
		select {
		case <-cg.ctx.Done():
			log.Println("readFromConnGoroutine: Context canceled")
			readErrCh <- utils.CastContextError(cg.ctx.Err())
			return
		default:
			resp, err := ReadResponse(conn)
			if err != nil {
				log.Printf("readFromConnGoroutine: Error reading Data: %v", err)
				readErrCh <- err
				return
			}
			cg.dispatchResponse(resp)
		}
	}
}

// dispatchResponse routes a response to the appropriate channel.
func (cg *ClientConnectionGroup) dispatchResponse(resp *ResponseMessage) {
	cg.responseMapMutex.Lock()
	defer cg.responseMapMutex.Unlock()

	ch, exists := cg.responseMap[resp.Header.RequestUid]
	if !exists {
		log.Printf("Response channel for RequestUid=%d not found", resp.Header.RequestUid)
		return
	}

	ch <- resp
	close(ch)
	delete(cg.responseMap, resp.Header.RequestUid)
}

func (cg *ClientConnectionGroup) SendMessage(ctx context.Context, msg *RequestMessage) error {
	log.Printf("SendMessage: Sending message with RequestUid=%d", msg.Header.RequestUid)
	select {
	case cg.messages <- msg:
		cg.responseMapMutex.Lock()
		if _, exists := cg.responseMap[msg.Header.RequestUid]; !exists {
			log.Printf("SendMessage: Creating response channel for RequestUid=%d", msg.Header.RequestUid)
			cg.responseMap[msg.Header.RequestUid] = make(chan *ResponseMessage, 1)
		}
		cg.responseMapMutex.Unlock()
		return nil
	case <-ctx.Done():
		log.Println("SendMessage: Context canceled")
		return utils.CastContextError(ctx.Err())
	}
}

func (cg *ClientConnectionGroup) WaitResponse(ctx context.Context, requestUid uint32) (*ResponseMessage, error) {
	cg.responseMapMutex.Lock()
	ch, exists := cg.responseMap[requestUid]
	cg.responseMapMutex.Unlock()
	if !exists {
		return nil, &utils.Error{
			Code: utils.ErrCodeResponseChannelNotFound,
			Msg:  "Response channel not found for given request ID",
			Tags: []string{utils.TagIllegalArgument},
		}
	}
	select {
	case <-ctx.Done():
		return nil, utils.CastContextError(ctx.Err())
	case resp := <-ch:
		return resp, nil
	}
}
