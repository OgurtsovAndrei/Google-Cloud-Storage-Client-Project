package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

type HandleRequestFunc func(ctx context.Context, r interface{}, connections *ClientConnectionPool) error

type ClientConnectionPool struct {
	clientID     string
	messages     chan *ResponseMessage
	nConnections uint32
	lock         sync.Mutex
	conns        map[net.Conn]bool
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewClientConnectionPool(clientID string, parentCtx context.Context) *ClientConnectionPool {
	ctx, cancel := context.WithCancel(parentCtx)
	return &ClientConnectionPool{
		clientID:     clientID,
		messages:     make(chan *ResponseMessage, 10),
		conns:        make(map[net.Conn]bool),
		nConnections: 0,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (clientPool *ClientConnectionPool) SendMessage(ctx context.Context, msg *ResponseMessage) {
	log.Printf("SendMessage: Sending message with RequestUid=%d", msg.Header.RequestUid)
	select {
	case clientPool.messages <- msg:
	case <-ctx.Done():
		log.Println("SendMessage: Context canceled")
	}
}

func (clientPool *ClientConnectionPool) writeToConnGoroutine(conn net.Conn, sgc *ServerConnectionGroup, clientUid string) {
	log.Println("Starting writeToConnGoroutine")
	for {
		select {
		case req := <-clientPool.messages:
			log.Printf("writeToConnGoroutine: Sending request for RequestUid=%d", req.Header.RequestUid)
			if _, err := io.Copy(conn, NewResponseReader(req)); err != nil {
				log.Printf("writeToConnGoroutine: Error writing request: %v", err)
				sgc.UnRegisterConnection(clientUid, conn)
				return
			}
		case <-clientPool.ctx.Done():
			log.Println("writeToConnGoroutine: Context canceled")
			return
		}
	}
}

type ServerConnectionGroup struct {
	address       string
	ctx           context.Context
	handleRequest HandleRequestFunc
	wg            sync.WaitGroup

	clientsSessionsMutex sync.Mutex
	clientsSessions      map[string]*ClientConnectionPool
}

func NewServerConnectionGroup(address string, ctx context.Context, handleRequest HandleRequestFunc) (*ServerConnectionGroup, error) {
	scg := &ServerConnectionGroup{
		address:         address,
		ctx:             ctx,
		handleRequest:   handleRequest,
		clientsSessions: make(map[string]*ClientConnectionPool),
	}

	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Printf("NewServerConnectionGroup: Не удалось слушать адрес %s: %v", address, err)
		return nil, err
	}

	go func() {
		<-ctx.Done()
		_ = l.Close()
		for _, sc := range scg.clientsSessions {
			sc.cancel()
		}
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

				r, err := ReadRequest(conn)
				if err != nil {
					log.Printf("NewServerConnectionGroup: Error ReadRequest: %v", err)
					return
				}

				var clientConnectionPool *ClientConnectionPool
				switch req := r.(type) {
				case *HandshakeRequest:
					clientConnectionPool = scg.RegisterConnection(req, conn)
				default:
					log.Printf("NewServerConnectionGroup: Error ReadRequest: %v", err)
					return
				}

				scg.wg.Add(1)
				go goHandleServerConnection(conn, scg, clientConnectionPool)
			}
		}
	}()

	return scg, nil
}

func goHandleServerConnection(c net.Conn, scg *ServerConnectionGroup, connPool *ClientConnectionPool) {
	defer scg.wg.Done()
	defer c.Close()
	scg.handleConnection(c, connPool)
}

func (scg *ServerConnectionGroup) cleanupConnPool(clientConn *ClientConnectionPool) {
	scg.clientsSessionsMutex.Lock()
	delete(scg.clientsSessions, clientConn.clientID)
	scg.clientsSessionsMutex.Unlock()

	clientConn.lock.Lock()
	defer clientConn.lock.Unlock()

	clientConn.cancel()
	for conn := range clientConn.conns {
		conn.Close()
		delete(clientConn.conns, conn)
	}
	clientConn.nConnections = 0
	for msg := range clientConn.messages {
		log.Printf("Drop message %s from %s", msg, clientConn.clientID)
	}
}

func (scg *ServerConnectionGroup) RegisterConnection(req *HandshakeRequest, conn net.Conn) *ClientConnectionPool {
	scg.clientsSessionsMutex.Lock()
	defer scg.clientsSessionsMutex.Unlock()

	clientConnPool, found := scg.clientsSessions[req.ClientID]
	if !found {
		clientConnPool = NewClientConnectionPool(req.ClientID, scg.ctx)
		scg.clientsSessions[req.ClientID] = clientConnPool
	}
	clientConnPool.writeToConnGoroutine(conn, scg, req.ClientID)
	clientConnPool.lock.Lock()
	defer clientConnPool.lock.Unlock()
	clientConnPool.conns[conn] = true
	clientConnPool.nConnections++
	log.Printf("Client %s: registered a new connection, total=%d",
		req.ClientID, clientConnPool.nConnections)

	response := BuildSucceedResponse(req.Header.RequestUid, "OK")
	clientConnPool.SendMessage(clientConnPool.ctx, &response)
	return clientConnPool
}

func (scg *ServerConnectionGroup) UnRegisterConnection(clientID string, conn net.Conn) {
	conn.Close()
	scg.clientsSessionsMutex.Lock()
	clientConnPool, found := scg.clientsSessions[clientID]
	scg.clientsSessionsMutex.Unlock()
	if !found {
		return
	}

	clientConnPool.lock.Lock()
	delete(clientConnPool.conns, conn)
	clientConnPool.nConnections--
	clientConnPool.lock.Unlock()

	if clientConnPool.nConnections == 0 {
		scg.cleanupConnPool(clientConnPool)
	}
}

func (scg *ServerConnectionGroup) handleConnection(conn net.Conn, connPool *ClientConnectionPool) {
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

			if err := scg.handleRequest(scg.ctx, r, connPool); err != nil {
				log.Printf("handleConnection: Error handling request: %v", err)
			}
		}
	}
}

func (scg *ServerConnectionGroup) Wait() {
	scg.wg.Wait()
}
