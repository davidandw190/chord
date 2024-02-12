package chord

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	internal "github.com/davidandw190/chord/internal"
	"google.golang.org/grpc"
)

var (
	emptyNode                = &internal.Node{}
	emptyRequest             = &internal.ER{}
	emptyGetResponse         = &internal.GetResponse{}
	emptySetResponse         = &internal.SetResponse{}
	emptyDeleteResponse      = &internal.DeleteResponse{}
	emptyRequestKeysResponse = &internal.RequestKeysResponse{}
)

// Transport interface defines the methods needed for a Chord ring.
type Transport interface {
	Start()
	GetSuccessor(*internal.Node) (*internal.Node, error)
	FindSuccessor(*internal.Node, []byte) (*internal.Node, error)
	GetPredecessor(*internal.Node) (*internal.Node, error)
	CheckPredecessor(*internal.Node) error
	Notify(*internal.Node, *internal.Node) error

	//Storage
	GetKey(*internal.Node, string) (*internal.GetResponse, error)
	SetKey(*internal.Node, string, string) error
	DeleteKey(*internal.Node, string) error
	RequestKeys(*internal.Node, []byte, []byte) ([]*internal.KV, error)
}

// GrpcTransport struct implements the Transport interface using gRPC.
type GrpcTransport struct {
	config *Config

	timeout time.Duration
	maxIdle time.Duration

	sock *net.TCPListener

	pool    map[string]*grpcConn
	poolMtx sync.RWMutex

	server *grpc.Server

	shutdown int32
}

// grpcConn represents a gRPC connection in the connection pool.
type grpcConn struct {
	addr       string
	client     internal.ChordClient
	conn       *grpc.ClientConn
	lastActive time.Time
}

// Close closes the gRPC connection.
func (g *grpcConn) Close() {
	g.conn.Close()
}

// Dial wraps grpc's dial function with settings that facilitate the functionality of transport.
func Dial(addr string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.Dial(addr, append(append(opts,
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
		grpc.FailOnNonTempDialError(true)),
		grpc.WithInsecure(),
	)...)
}

// GetServer returns the gRPC server associated with the transport.
func (g *GrpcTransport) GetServer() *grpc.Server {
	return g.server
}

// Start starts the RPC server and reaps old connections.
func (g *GrpcTransport) Start() {
	go g.listen()
	go g.reapOld()
}

// NewGrpcTransport creates a new instance of GrpcTransport.
func NewGrpcTransport(config *Config) (*GrpcTransport, error) {
	addr := config.Addr
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	pool := make(map[string]*grpcConn)
	grp := &GrpcTransport{
		sock:    listener.(*net.TCPListener),
		timeout: config.Timeout,
		maxIdle: config.MaxIdle,
		pool:    pool,
		server:  grpc.NewServer(),
	}
	return grp, nil
}

// Shutdown shuts down the TCP transport, closing all connections.
func (g *GrpcTransport) Shutdown() {
	atomic.StoreInt32(&g.shutdown, 1)
	g.poolMtx.Lock()
	g.server.Stop()
	for _, conn := range g.pool {
		conn.Close()
	}
	g.pool = nil
	g.poolMtx.Unlock()
}

// GetSuccessor retrieves the successor ID of a remote node.
func (g *GrpcTransport) GetSuccessor(node *internal.Node) (*internal.Node, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.GetSuccessor(ctx, emptyRequest)
}

// FindSuccessor finds the successor ID of a remote node.
func (g *GrpcTransport) FindSuccessor(node *internal.Node, id []byte) (*internal.Node, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.FindSuccessor(ctx, &internal.ID{Id: id})
}

// GetPredecessor retrieves the predecessor ID of a remote node.
func (g *GrpcTransport) GetPredecessor(node *internal.Node) (*internal.Node, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.GetPredecessor(ctx, emptyRequest)
}

// Notify sends a notification to a remote node.
func (g *GrpcTransport) Notify(node, pred *internal.Node) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.Notify(ctx, pred)
	return err
}

func (g *GrpcTransport) CheckPredecessor(node *internal.Node) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.CheckPredecessor(ctx, &internal.ID{Id: node.Id})
	return err
}

func (g *GrpcTransport) GetKey(node *internal.Node, key string) (*internal.GetResponse, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	return client.XGet(ctx, &internal.GetRequest{Key: key})
}

func (g *GrpcTransport) SetKey(node *internal.Node, key, value string) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.XSet(ctx, &internal.SetRequest{Key: key, Value: value})
	return err
}

func (g *GrpcTransport) DeleteKey(node *internal.Node, key string) error {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	_, err = client.XDelete(ctx, &internal.DeleteRequest{Key: key})
	return err
}
func (g *GrpcTransport) RequestKeys(node *internal.Node, from, to []byte) ([]*internal.KV, error) {
	client, err := g.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	val, err := client.XRequestKeys(
		ctx, &internal.RequestKeysRequest{From: from, To: to},
	)
	if err != nil {
		return nil, err
	}
	return val.Values, nil
}

func (g *GrpcTransport) registerNode(node *Node) {
	internal.RegisterChordServer(g.server, node)
}

// getConn gets an outbound connection to a host.
func (g *GrpcTransport) getConn(addr string) (internal.ChordClient, error) {
	g.poolMtx.RLock()

	if atomic.LoadInt32(&g.shutdown) == 1 {
		g.poolMtx.Unlock()
		return nil, fmt.Errorf("TCP transport is shutdown")
	}

	cc, ok := g.pool[addr]
	g.poolMtx.RUnlock()
	if ok {
		return cc.client, nil
	}

	conn, err := Dial(addr)
	if err != nil {
		return nil, err
	}

	client := internal.NewChordClient(conn)
	cc = &grpcConn{addr, client, conn, time.Now()}
	g.poolMtx.Lock()

	if g.pool == nil {
		g.poolMtx.Unlock()
		return nil, errors.New("must instantiate node before usage")
	}

	g.pool[addr] = cc
	g.poolMtx.Unlock()

	return client, nil
}

// returnConn returns an outbound TCP connection to the pool.
func (g *GrpcTransport) returnConn(o *grpcConn) {
	o.lastActive = time.Now()
	g.poolMtx.Lock()
	defer g.poolMtx.Unlock()
	if atomic.LoadInt32(&g.shutdown) == 1 {
		o.conn.Close()
		return
	}
	g.pool[o.addr] = o
}

// reapOld closes old outbound connections.
func (g *GrpcTransport) reapOld() {
	for {
		if atomic.LoadInt32(&g.shutdown) == 1 {
			return
		}
		time.Sleep(30 * time.Second)
		g.reapOnce()
	}
}

// reapOnce removes old connections from the pool.
func (g *GrpcTransport) reapOnce() {
	g.poolMtx.Lock()
	defer g.poolMtx.Unlock()
	for host, conn := range g.pool {
		if time.Since(conn.lastActive) > g.maxIdle {
			conn.Close()
			delete(g.pool, host)
		}
	}
}

// listen listens for inbound connections.
func (g *GrpcTransport) listen() {
	g.server.Serve(g.sock)
}
