package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

func (rm *RequestMessage) Read(p []byte) (n int, err error) {
	log.Println("RequestMessage: Starting Read")
	headerBuf := make([]byte, binary.Size(rm.header))
	err = binary.Write(io.Discard, binary.LittleEndian, rm.header)
	if err != nil {
		log.Printf("RequestMessage: Error writing header: %v", err)
		return 0, err
	}
	copy(p, headerBuf)
	n += len(headerBuf)

	if rm.secondHeader != nil {
		nSecond, err := rm.secondHeader.Read(p[n:])
		n += nSecond
		if err != nil && err != io.EOF {
			log.Printf("RequestMessage: Error reading secondHeader: %v", err)
			return n, err
		}
		if err == io.EOF && rm.data != nil {
			nData, err := rm.data.Read(p[n:])
			n += nData
			return n, err
		}
	}
	if rm.data != nil {
		nData, err := rm.data.Read(p[n:])
		n += nData
		return n, err
	}
	log.Println("RequestMessage: Finished Read with EOF")
	return n, io.EOF
}

func (resp *ResponseMessage) Read(p []byte) (n int, err error) {
	log.Println("ResponseMessage: Starting Read")
	headerBuf := make([]byte, binary.Size(resp.header))
	err = binary.Write(io.Discard, binary.LittleEndian, resp.header)
	if err != nil {
		log.Printf("ResponseMessage: Error writing header: %v", err)
		return 0, err
	}
	copy(p, headerBuf)
	n += len(headerBuf)

	if resp.secondHeader != nil {
		nSecond, err := resp.secondHeader.Read(p[n:])
		n += nSecond
		if err != nil && err != io.EOF {
			log.Printf("ResponseMessage: Error reading secondHeader: %v", err)
			return n, err
		}
		if err == io.EOF && resp.data != nil {
			nData, err := resp.data.Read(p[n:])
			n += nData
			return n, err
		}
	}
	if resp.data != nil {
		nData, err := resp.data.Read(p[n:])
		n += nData
		return n, err
	}
	log.Println("ResponseMessage: Finished Read with EOF")
	return n, io.EOF
}

type ConnectionGroup struct {
	messages       chan *RequestMessage
	responseMap    map[uint32]chan *ResponseMessage
	responseMapMux sync.Mutex
	address        string
	ctx            context.Context
	nConnections   int
}

func NewConnectionGroup(bufferSize int, address string, ctx context.Context, nConnections int) *ConnectionGroup {
	log.Printf("Creating ConnectionGroup with bufferSize=%d, address=%s, nConnections=%d", bufferSize, address, nConnections)
	cg := &ConnectionGroup{
		messages:     make(chan *RequestMessage, bufferSize),
		responseMap:  make(map[uint32]chan *ResponseMessage),
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
	log.Println("Starting readFromConn")
	for {
		select {
		case <-cg.ctx.Done():
			log.Println("readFromConn: Context canceled")
			readErrCh <- cg.ctx.Err()
			return
		default:
			var resp ResponseMessage
			if err := binary.Read(conn, binary.LittleEndian, &resp.header); err != nil {
				log.Printf("readFromConn: Error reading header: %v", err)
				readErrCh <- err
				return
			}

			cg.responseMapMux.Lock()
			if ch, exists := cg.responseMap[resp.header.RequestUid]; exists {
				log.Printf("readFromConn: Sending response for RequestUid=%d", resp.header.RequestUid)
				ch <- &resp
				close(ch)
				delete(cg.responseMap, resp.header.RequestUid)
			}
			cg.responseMapMux.Unlock()
		}
	}
}

func (cg *ConnectionGroup) writeToConn(conn net.Conn, writeErrCh chan<- error) {
	log.Println("Starting writeToConn")
	for {
		select {
		case req := <-cg.messages:
			log.Printf("writeToConn: Sending request for RequestUid=%d", req.header.RequestUid)
			if _, err := io.Copy(conn, req); err != nil {
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
	log.Printf("SendMessage: Sending message with RequestUid=%d", msg.header.RequestUid)
	select {
	case cg.messages <- msg:
		cg.responseMapMux.Lock()
		if _, exists := cg.responseMap[msg.header.RequestUid]; !exists {
			log.Printf("SendMessage: Creating response channel for RequestUid=%d", msg.header.RequestUid)
			cg.responseMap[msg.header.RequestUid] = make(chan *ResponseMessage, 1)
		}
		cg.responseMapMux.Unlock()
		return nil
	case <-ctx.Done():
		log.Println("SendMessage: Context canceled")
		return ctx.Err()
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
