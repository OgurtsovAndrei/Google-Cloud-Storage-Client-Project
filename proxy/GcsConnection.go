package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

type requestReader struct {
	headerBuffer  *bytes.Buffer
	secondHeader  io.Reader
	data          io.Reader
	currentReader io.Reader
}

func NewRequestReader(req *RequestMessage) io.Reader {
	log.Println("NewResponseReader: Create Request Reader")

	headerBuf := new(bytes.Buffer)
	_ = binary.Write(headerBuf, binary.BigEndian, req.Header)
	return &requestReader{
		headerBuffer:  headerBuf,
		secondHeader:  req.SecondHeader,
		data:          req.Data,
		currentReader: headerBuf,
	}
}

func (r *requestReader) Read(p []byte) (n int, err error) {
	log.Println("RequestMessage: Starting Read")
	for {
		if r.currentReader == nil {
			return n, io.EOF
		}
		m, err := r.currentReader.Read(p[n:])
		log.Printf("requestReader: Read %d firs bytes: %x", m, p[n:n+min(m, 40)])
		n += m
		if err == io.EOF {
			if r.currentReader == r.headerBuffer {
				r.currentReader = r.secondHeader
			} else if r.currentReader == r.secondHeader {
				r.currentReader = r.data
			} else {
				r.currentReader = nil
			}
			if n > 0 {
				return n, nil
			}
		} else if err != nil {
			return n, err
		} else {
			return n, nil
		}
	}
}

type responseReader struct {
	headerBuffer  *bytes.Buffer
	data          io.Reader
	currentReader io.Reader
}

func NewResponseReader(resp *ResponseMessage) io.Reader {
	log.Println("NewResponseReader: Create Response Reader")
	headerBuf := new(bytes.Buffer)
	_ = binary.Write(headerBuf, binary.BigEndian, resp.Header)
	return &responseReader{
		headerBuffer:  headerBuf,
		data:          strings.NewReader(resp.Data),
		currentReader: headerBuf,
	}
}

func (r *responseReader) Read(p []byte) (n int, err error) {
	log.Println("Response Message: Starting Read")
	for {
		if r.currentReader == nil {
			return n, io.EOF
		}
		m, err := r.currentReader.Read(p[n:])
		log.Printf("responseReader: Read %d bytes: %x", m, p[n:n+m])
		n += m
		if err == io.EOF {
			if r.currentReader == r.headerBuffer {
				r.currentReader = r.data
			} else {
				r.currentReader = nil
			}
			if n > 0 {
				return n, nil
			}
		} else if err != nil {
			return n, err
		} else {
			return n, nil
		}
	}
}

func SendSuccessResponse(conn io.Writer, requestUid uint32, message string) {
	fmt.Printf("Sending operation success response: %s\n", message)
	respHeader := ResponseHeader{
		RequestUid: requestUid,
		StatusCode: 0,
		DataLength: uint32(len(message)),
	}

	resp := ResponseMessage{
		Header: respHeader,
		Data:   message,
	}

	_, err := io.Copy(conn, NewResponseReader(&resp))
	if err != nil {
		log.Printf("Sending operation success response failed: %s\n", err)
	}
}

func SendErrorResponse(conn io.Writer, requestUid uint32, err error) {
	if err == nil {
		SendSuccessResponse(conn, requestUid, "")
		return
	}

	msg := err.Error()
	respHeader := ResponseHeader{
		RequestUid: requestUid,
		StatusCode: 1,
		DataLength: uint32(len(msg)),
	}

	resp := ResponseMessage{
		Header: respHeader,
		Data:   msg,
	}
	_, err = io.Copy(conn, NewResponseReader(&resp))
	if err != nil {
		log.Printf("SendErrorResponse Write Error: %s", err)
	}
}

type ConnectionGroup struct {
	messages       chan *RequestMessage
	ResponseMap    map[uint32]chan *ResponseMessage
	ResponseMapMux sync.Mutex
	address        string
	ctx            context.Context
	nConnections   int
}

func NewConnectionGroup(bufferSize int, address string, ctx context.Context, nConnections int) *ConnectionGroup {
	log.Printf("Creating ConnectionGroup with bufferSize=%d, address=%s, nConnections=%d", bufferSize, address, nConnections)
	cg := &ConnectionGroup{
		messages:     make(chan *RequestMessage, bufferSize),
		ResponseMap:  make(map[uint32]chan *ResponseMessage),
		address:      address,
		ctx:          ctx,
		nConnections: nConnections,
	}

	for i := 0; i < nConnections; i++ {
		go func(i int) {
			log.Printf("Starting connection goroutine %d", i)
			if err := cg.handleConnection(); err != nil {
				log.Printf("Connection goroutine %d error: %v", i, err)
			}
		}(i)
	}

	return cg
}

func (cg *ConnectionGroup) createConnection() (net.Conn, error) {
	log.Printf("Creating connection to %s", cg.address)
	return net.Dial("tcp", cg.address)
}

func (cg *ConnectionGroup) readFromConn(conn net.Conn, readErrCh chan<- error) {
	log.Println("c")
	for {
		select {
		case <-cg.ctx.Done():
			log.Println("readFromConn: Context canceled")
			readErrCh <- cg.ctx.Err()
			return
		default:
			var resp ResponseMessage
			if err := binary.Read(conn, binary.BigEndian, &resp.Header); err != nil {
				log.Printf("readFromConn: Error reading Header: %v", err)
				readErrCh <- err
				return
			}
			data := make([]byte, resp.Header.DataLength)
			_, err := conn.Read(data)

			if err != nil {
				log.Printf("readFromConn: Error reading Data: %v", err)
				return
			}

			resp.Data = string(data)
			cg.ResponseMapMux.Lock()

			if ch, exists := cg.ResponseMap[resp.Header.RequestUid]; exists {
				log.Printf("readFromConn: Sending response for RequestUid=%d", resp.Header.RequestUid)
				ch <- &resp
				close(ch)
				delete(cg.ResponseMap, resp.Header.RequestUid)
			} else {
				log.Printf("readFromConn: RequestUid=%d not found in ResponseMap", resp.Header.RequestUid)
			}
			cg.ResponseMapMux.Unlock()
		}
	}
}

func (cg *ConnectionGroup) writeToConn(conn net.Conn, writeErrCh chan<- error) {
	log.Println("Starting writeToConn")
	for {
		select {
		case req := <-cg.messages:
			log.Printf("writeToConn: Sending request for RequestUid=%d", req.Header.RequestUid)
			if _, err := io.Copy(conn, NewRequestReader(req)); err != nil {
				log.Printf("writeToConn: Error writing request: %v", err)
				writeErrCh <- err
				return
			}
		case <-cg.ctx.Done():
			log.Println("writeToConn: Context canceled")
			writeErrCh <- cg.ctx.Err()
			return
		}
	}
}

func (cg *ConnectionGroup) handleConnection() error {
	log.Println("Handling connection")
	conn, err := cg.createConnection()
	if err != nil {
		log.Printf("handleConnection: Error creating connection: %v", err)
		return err
	}
	defer conn.Close()

	readErrCh := make(chan error, 1)
	writeErrCh := make(chan error, 1)

	go cg.readFromConn(conn, readErrCh)
	go cg.writeToConn(conn, writeErrCh)

	select {
	case <-cg.ctx.Done():
		log.Println("handleConnection: Context canceled")
		return cg.ctx.Err()
	case err := <-readErrCh:
		log.Printf("handleConnection: Read error: %v", err)
		return err
	case err := <-writeErrCh:
		log.Printf("handleConnection: Write error: %v", err)
		return err
	}
}

func (cg *ConnectionGroup) SendMessage(ctx context.Context, msg *RequestMessage) error {
	log.Printf("SendMessage: Sending message with RequestUid=%d", msg.Header.RequestUid)
	select {
	case cg.messages <- msg:
		cg.ResponseMapMux.Lock()
		if _, exists := cg.ResponseMap[msg.Header.RequestUid]; !exists {
			log.Printf("SendMessage: Creating response channel for RequestUid=%d", msg.Header.RequestUid)
			cg.ResponseMap[msg.Header.RequestUid] = make(chan *ResponseMessage, 1)
		}
		cg.ResponseMapMux.Unlock()
		return nil
	case <-ctx.Done():
		log.Println("SendMessage: Context canceled")
		return ctx.Err()
	}
}

func (cg *ConnectionGroup) WaitResponse(ctx context.Context, requestUid uint32) (*ResponseMessage, error) {
	cg.ResponseMapMux.Lock()
	ch, exists := cg.ResponseMap[requestUid]
	cg.ResponseMapMux.Unlock()
	if !exists {
		return nil, errors.New("response channel not found")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case resp := <-ch:
		return resp, nil
	}
}

// Server

type HandleRequestFunc func(ctx context.Context, r interface{}, conn net.Conn) error

type ServerConnectionGroup struct {
	address       string
	ctx           context.Context
	handleRequest HandleRequestFunc
	wg            sync.WaitGroup
}

func NewServerConnectionGroup(address string, ctx context.Context, handleRequest HandleRequestFunc) (*ServerConnectionGroup, error) {
	scg := &ServerConnectionGroup{
		address:       address,
		ctx:           ctx,
		handleRequest: handleRequest,
	}

	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Printf("NewServerConnectionGroup: Не удалось слушать адрес %s: %v", address, err)
		return nil, err
	}

	go func() {
		<-ctx.Done()
		_ = l.Close()
		fmt.Println("GCSProxyServer has been shut down.")
	}()

	go func() {
		log.Printf("Server listening on %s...", address)
		for {
			select {
			case <-ctx.Done():
				log.Println("NewServerConnectionGroup: Contex Done, stopping accepting connections...")
				_ = l.Close()
				return
			default:
				conn, err := l.Accept()
				if err != nil {
					select {
					case <-ctx.Done():
						log.Println("NewServerConnectionGroup: Context closed, aborting accept...")
						return
					default:
						log.Printf("NewServerConnectionGroup: Error Accept: %v", err)
						continue
					}
				}
				scg.wg.Add(1)
				go func(c net.Conn) {
					defer scg.wg.Done()
					defer c.Close()
					scg.handleConnection(c)
				}(conn)
			}
		}
	}()

	return scg, nil
}

func (scg *ServerConnectionGroup) handleConnection(conn net.Conn) {
	log.Printf("handleConnection: Starting to process new connection from %s", conn.RemoteAddr().String())

	for {
		select {
		case <-scg.ctx.Done():
			log.Println("handleConnection: Context canceled, exiting")
			return
		default:
			r, err := ReadRequest(conn)
			if err != nil {
				if err == io.EOF {
					log.Printf("handleConnection: Client %s closed the connection", conn.RemoteAddr().String())
				} else {
					log.Printf("handleConnection: Error reading request: %v", err)
				}
				return
			}

			if err := scg.handleRequest(scg.ctx, r, conn); err != nil {
				log.Printf("handleConnection: Error handling request: %v", err)
			}

			log.Printf("handleConnection: Successfully handled request")
		}
	}
}

func (scg *ServerConnectionGroup) Wait() {
	scg.wg.Wait()
}
