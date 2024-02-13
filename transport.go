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
	Start() error
	Stop() error

	// RPC
	GetSuccessor(node *internal.Node) (*internal.Node, error)
	FindSuccessor(node *internal.Node, id []byte) (*internal.Node, error)
	GetPredecessor(node *internal.Node) (*internal.Node, error)
	Notify(node, pred *internal.Node) error
	CheckPredecessor(node *internal.Node) error
	SetPredecessor(node, pred *internal.Node) error
	SetSuccessor(node, succ *internal.Node) error

	// Storage
	GetKey(node *internal.Node, key string) (*internal.GetResponse, error)
	SetKey(node *internal.Node, key, value string) error
	DeleteKey(node *internal.Node, key string) error
	RequestKeys(node *internal.Node, from, to []byte) ([]*internal.KV, error)
	DeleteKeys(node *internal.Node, keys []string) error
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
func (gc *grpcConn) Close() {
	gc.conn.Close()
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

// NewGrpcTransport creates a new instance of GrpcTransport with the given configuration.
func NewGrpcTransport(config *Config) (*GrpcTransport, error) {
	listener, err := net.Listen("tcp", config.Addr)
	if err != nil {
		return nil, err
	}

	pool := make(map[string]*grpcConn)

	grpcTransport := &GrpcTransport{
		sock:    listener.(*net.TCPListener),
		timeout: config.Timeout,
		maxIdle: config.MaxIdle,
		pool:    pool,
		config:  config,
	}

	grpcTransport.server = grpc.NewServer(config.ServerOpts...)

	return grpcTransport, nil
}

// GetServer returns the underlying gRPC server.
func (gt *GrpcTransport) GetServer() *grpc.Server {
	return gt.server
}

// Gets an outbound connection to a host.
func (gt *GrpcTransport) getConn(addr string) (internal.ChordClient, error) {
	gt.poolMtx.RLock()

	if atomic.LoadInt32(&gt.shutdown) == 1 {
		gt.poolMtx.Unlock()
		return nil, fmt.Errorf("TCP transport is shutdown")
	}

	cc, ok := gt.pool[addr]
	gt.poolMtx.RUnlock()
	if ok {
		return cc.client, nil
	}

	conn, err := Dial(addr, gt.config.DialOpts...)
	if err != nil {
		return nil, err
	}

	client := internal.NewChordClient(conn)
	cc = &grpcConn{addr, client, conn, time.Now()}
	gt.poolMtx.Lock()
	if gt.pool == nil {
		gt.poolMtx.Unlock()
		return nil, errors.New("must instantiate node before using")
	}
	gt.pool[addr] = cc
	gt.poolMtx.Unlock()

	return client, nil
}

// Start starts the gRPC server and begins reaping old connections.
func (gt *GrpcTransport) Start() error {
	go gt.listen()
	go gt.reapOld()

	return nil
}

// ReturnConn returns an outbound TCP connection to the pool.
func (gt *GrpcTransport) returnConn(gc *grpcConn) {
	gc.lastActive = time.Now()

	gt.poolMtx.Lock()
	defer gt.poolMtx.Unlock()
	if atomic.LoadInt32(&gt.shutdown) == 1 {
		gc.conn.Close()
		return
	}
	gt.pool[gc.addr] = gc
}

// Stop shuts down the TCP transport and closes all connections.
func (gt *GrpcTransport) Stop() error {
	atomic.StoreInt32(&gt.shutdown, 1)

	gt.poolMtx.Lock()
	defer gt.poolMtx.Unlock()

	gt.server.Stop()
	for _, conn := range gt.pool {
		conn.Close()
	}
	gt.pool = nil

	return nil
}

// ReapOld closes old outbound connections.
func (gt *GrpcTransport) reapOld() {
	ticker := time.NewTicker(60 * time.Second)

	for {
		if atomic.LoadInt32(&gt.shutdown) == 1 {
			return
		}
		select {
		case <-ticker.C:
			gt.reap()
		}
	}
}

// Reap closes old outbound connections from the connection pool.
func (gt *GrpcTransport) reap() {
	gt.poolMtx.Lock()
	defer gt.poolMtx.Unlock()

	for host, conn := range gt.pool {
		if time.Since(conn.lastActive) > gt.maxIdle {
			conn.Close()
			delete(gt.pool, host)
		}
	}
}

// Listen listens for inbound connections.
func (gt *GrpcTransport) listen() {
	gt.server.Serve(gt.sock)
}

// GetSuccessor gets the successor ID of a remote node.
func (gt *GrpcTransport) GetSuccessor(node *internal.Node) (*internal.Node, error) {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	return client.GetSuccessor(ctx, emptyRequest)
}

// FindSuccessor finds the successor ID of a remote node.
func (gt *GrpcTransport) FindSuccessor(node *internal.Node, id []byte) (*internal.Node, error) {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	return client.FindSuccessor(ctx, &internal.ID{Id: id})
}

// GetPredecessor gets the predecessor ID of a remote node.
func (gt *GrpcTransport) GetPredecessor(node *internal.Node) (*internal.Node, error) {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	return client.GetPredecessor(ctx, emptyRequest)
}

// SetPredecessor sets the predecessor ID of a remote node.
func (gt *GrpcTransport) SetPredecessor(node, pred *internal.Node) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.SetPredecessor(ctx, pred)
	return err
}

// SetSuccessor sets the successor ID of a remote node.
func (gt *GrpcTransport) SetSuccessor(node, succ *internal.Node) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.SetSuccessor(ctx, succ)
	return err
}

// Notify notifies a remote node of its predecessor.
func (gt *GrpcTransport) Notify(node, pred *internal.Node) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.Notify(ctx, pred)
	return err
}

// CheckPredecessor checks the predecessor of a remote node.
func (gt *GrpcTransport) CheckPredecessor(node *internal.Node) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.CheckPredecessor(ctx, &internal.ID{Id: node.Id})
	return err
}

// GetKey retrieves a key from a remote node.
func (gt *GrpcTransport) GetKey(node *internal.Node, key string) (*internal.GetResponse, error) {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	return client.XGet(ctx, &internal.GetRequest{Key: key})
}

// SetKey sets a key-value pair on a remote node.
func (gt *GrpcTransport) SetKey(node *internal.Node, key, value string) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.XSet(ctx, &internal.SetRequest{Key: key, Value: value})
	return err
}

// DeleteKey deletes a key from a remote node.
func (gt *GrpcTransport) DeleteKey(node *internal.Node, key string) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.XDelete(ctx, &internal.DeleteRequest{Key: key})
	return err
}

// RequestKeys retrieves keys in a specific range from a remote node.
func (gt *GrpcTransport) RequestKeys(node *internal.Node, from, to []byte) ([]*internal.KV, error) {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	val, err := client.XRequestKeys(
		ctx, &internal.RequestKeysRequest{From: from, To: to},
	)
	if err != nil {
		return nil, err
	}
	return val.Values, nil
}

// DeleteKeys deletes multiple keys from a remote node.
func (gt *GrpcTransport) DeleteKeys(node *internal.Node, keys []string) error {
	client, err := gt.getConn(node.Addr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), gt.timeout)
	defer cancel()
	_, err = client.XMultiDelete(
		ctx, &internal.MultiDeleteRequest{Keys: keys},
	)
	return err
}
