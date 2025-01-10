package proxy

import (
	"context"
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

func (clientPool *ClientConnectionPool) SendResponseMessage(ctx context.Context, msg *ResponseMessage) {
	log.Printf("SERVER: SendResponseMessage: Sending message with RequestUid=%d", msg.Header.RequestUid)
	select {
	case clientPool.messages <- msg:
		log.Printf("SERVER: SendResponseMessage: Sent response message with RequestUid=%d", msg.Header.RequestUid)
	case <-ctx.Done():
		log.Println("SERVER: SendResponseMessage: Context canceled")
	}
}

func (clientPool *ClientConnectionPool) writeToConnGoroutine(conn net.Conn, sgc *ServerConnectionGroup, clientUid string) {
	log.Println("SERVER: Server: Starting writeToConnGoroutine")
	for {
		select {
		case req := <-clientPool.messages:
			log.Printf("SERVER: writeToConnGoroutine: Sending response for RequestUid=%d, Body=%s", req.Header.RequestUid, req.Data)
			if _, err := io.Copy(conn, NewResponseReader(req)); err != nil {
				clientPool.messages <- req
				log.Printf("SERVER: writeToConnGoroutine: Error writing request: %v", err)
				sgc.UnRegisterConnection(clientUid, conn)
				return
			}

			log.Printf("SERVER: writeToConnGoroutine: Successfully sent response for RequestUid=%d, Body=%s", req.Header.RequestUid, req.Data)
		case <-clientPool.ctx.Done():
			log.Println("SERVER: writeToConnGoroutine: Context canceled")
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
		log.Printf("SERVER: NewServerConnectionGroup: Не удалось слушать адрес %s: %v", address, err)
		return nil, err
	}

	go func() {
		<-ctx.Done()
		_ = l.Close()
		for _, sc := range scg.clientsSessions {
			sc.cancel()
		}
		log.Println("SERVER: GCSProxyServer has been shut down.")
	}()

	go func() {
		log.Printf("SERVER: Server listening on %s...", address)
		for {
			select {
			case <-ctx.Done():
				log.Println("SERVER: NewServerConnectionGroup: Contex Done, stopping accepting connections...")
				_ = l.Close()
				return
			default:
				conn, err := l.Accept()
				if err != nil {
					select {
					case <-ctx.Done():
						log.Println("SERVER: NewServerConnectionGroup: Context closed, aborting accept...")
						return
					default:
						log.Printf("SERVER: NewServerConnectionGroup: Error Accept: %v", err)
						continue
					}
				}

				r, err := ReadRequest(conn)
				if err != nil {
					log.Printf("SERVER: NewServerConnectionGroup: Error ReadRequest: %v", err)
					return
				}

				var clientConnectionPool *ClientConnectionPool
				switch req := r.(type) {
				case *HandshakeRequest:
					clientConnectionPool = scg.RegisterConnection(req, conn)
				default:
					log.Printf("SERVER: NewServerConnectionGroup: Error ReadRequest: %v", err)
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
		log.Printf("SERVER: Drop message %s from %s", msg, clientConn.clientID)
	}
}

func (scg *ServerConnectionGroup) RegisterConnection(req *HandshakeRequest, conn net.Conn) *ClientConnectionPool {
	log.Printf("SERVER: RegisterConnection: Attempting to register connection for ClientID=%s", req.ClientID)
	scg.clientsSessionsMutex.Lock()
	defer scg.clientsSessionsMutex.Unlock()

	clientConnPool, found := scg.clientsSessions[req.ClientID]
	if !found {
		log.Printf("SERVER: RegisterConnection: Creating new ClientConnectionPool for ClientID=%s", req.ClientID)
		clientConnPool = NewClientConnectionPool(req.ClientID, scg.ctx)
		scg.clientsSessions[req.ClientID] = clientConnPool
	} else {
		log.Printf("SERVER: RegisterConnection: Existing ClientConnectionPool found for ClientID=%s", req.ClientID)
	}

	go clientConnPool.writeToConnGoroutine(conn, scg, req.ClientID)
	clientConnPool.lock.Lock()
	defer clientConnPool.lock.Unlock()
	clientConnPool.conns[conn] = true
	clientConnPool.nConnections++
	log.Printf("SERVER: RegisterConnection: Client %s registered a new connection. Total connections=%d", req.ClientID, clientConnPool.nConnections)

	SendSuccessResponse(conn, req.Header.RequestUid, "OK")
	log.Printf("SERVER: RegisterConnection: Success response sent to ClientID=%s", req.ClientID)
	return clientConnPool
}

func (scg *ServerConnectionGroup) UnRegisterConnection(clientID string, conn net.Conn) {
	log.Printf("SERVER: UnRegisterConnection: Unregistering connection for ClientID=%s", clientID)
	conn.Close()
	scg.clientsSessionsMutex.Lock()
	clientConnPool, found := scg.clientsSessions[clientID]
	scg.clientsSessionsMutex.Unlock()
	if !found {
		log.Printf("SERVER: UnRegisterConnection: No ClientConnectionPool found for ClientID=%s", clientID)
		return
	}

	clientConnPool.lock.Lock()
	delete(clientConnPool.conns, conn)
	clientConnPool.nConnections--
	log.Printf("SERVER: UnRegisterConnection: Removed connection for ClientID=%s. Remaining connections=%d", clientID, clientConnPool.nConnections)
	clientConnPool.lock.Unlock()

	if clientConnPool.nConnections == 0 {
		log.Printf("SERVER: UnRegisterConnection: No active connections left for ClientID=%s. Cleaning up connection pool.", clientID)
		scg.cleanupConnPool(clientConnPool)
	}
}

func (scg *ServerConnectionGroup) handleConnection(conn net.Conn, connPool *ClientConnectionPool) {
	log.Printf("SERVER: handleConnection: Starting to process new connection from %s", conn.RemoteAddr().String())

	for {
		select {
		case <-scg.ctx.Done():
			log.Println("SERVER: handleConnection: Context canceled, exiting")
			return
		default:
			r, err := ReadRequest(conn)
			if err != nil {
				if err == io.EOF {
					log.Printf("SERVER: handleConnection: Client %s closed the connection", conn.RemoteAddr().String())
				} else {
					log.Printf("SERVER: handleConnection: Error reading request: %v", err)
				}
				return
			}

			if err := scg.handleRequest(scg.ctx, r, connPool); err != nil {
				log.Printf("SERVER: handleConnection: Error handling request: %v", err)
			}
		}
	}
}

func (scg *ServerConnectionGroup) Wait() {
	scg.wg.Wait()
}
