package proxy

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

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
		}
	}
}

func (scg *ServerConnectionGroup) Wait() {
	scg.wg.Wait()
}
