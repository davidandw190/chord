package chord

import (
	"context"
	"fmt"
	"hash"
	"sync"
	"time"

	internal "github.com/davidandw190/chord/internal"
	"google.golang.org/grpc"
)

// Config represents the configuration options for a Chord node.
type Config struct {
	Id           []byte
	Addr         string
	ServerOpts   []grpc.ServerOption
	DialOpts     []grpc.DialOption
	HashFunc     func() hash.Hash // Hash function to use
	StabilizeMin time.Duration    // Minimum stabilization time
	StabilizeMax time.Duration    // Maximum stabilization time
	Timeout      time.Duration
	MaxIdle      time.Duration
}

// Storage interface defines methods for interacting with storage.
type Storage interface {
	Get(string) string
	Put(string, string) error
}

// Node represents a Chord node.
type Node struct {
	*internal.Node

	predecessor *internal.Node
	predMtx     sync.RWMutex

	successor *internal.Node
	succMtx   sync.RWMutex

	shutdownCh chan struct{}

	fingerTable fingerTable
	ftMtx       sync.RWMutex

	storage Storage
	stMtx   sync.RWMutex

	transport Transport
	tsMtx     sync.RWMutex

	lastStablized time.Time
}

// DefaultConfig returns a default configuration for Chord nodes.
func DefaultConfig() *Config {
	return &Config{
		ServerOpts: make([]grpc.ServerOption, 1),
		DialOpts:   make([]grpc.DialOption, 1),
	}
}

// NewInode creates a new internal node with the given ID and address.
func NewInode(id []byte, addr string) *internal.Node {
	return &internal.Node{
		Id:   id,
		Addr: addr,
	}
}

// NewNode creates a Chord node with the specified configuration and join node.
func NewNode(cnf *Config, joinNode *internal.Node) (*Node, error) {
	node := &Node{
		Node:       new(internal.Node),
		shutdownCh: make(chan struct{}),
	}

	if cnf.Id != nil {
		node.Node.Id = cnf.Id
	} else {
		id, err := hashKey(cnf.Addr)
		if err != nil {
			return nil, err
		}
		node.Node.Id = id
	}
	node.Node.Addr = cnf.Addr

	// Populate finger table
	node.fingerTable = newFingerTable(node.Node)

	// Start RPC server
	transport, err := NewGrpcTransport(cnf)
	if err != nil {
		return nil, err
	}

	node.transport = transport

	internal.RegisterChordServer(transport.server, node)

	node.transport.Start()

	if err := node.join(joinNode); err != nil {
		return nil, err
	}

	// thread 1: kick off timer to stabilize periodically
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		for {
			select {
			case <-ticker.C:
				node.stabilize()
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// thread 2: kick off timer to fix finger table periodically
	go func() {
		next := 0
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				next = node.fixNextFinger(next)
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	// thread 3: CheckPredecessor checkes whether predecessor has failed.

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				node.checkPredecessor()
			case <-node.shutdownCh:
				ticker.Stop()
				return
			}
		}
	}()

	return node, nil
}

// GetSuccessor gets the successor node of the current node.
func (n *Node) GetSuccessor(ctx context.Context, r *internal.ER) (*internal.Node, error) {
	n.succMtx.RLock()
	succ := n.successor
	n.succMtx.RUnlock()
	if succ == nil {
		return emptyNode, nil
	}

	return succ, nil
}

// FindSuccessor finds the successor node for a given ID.
func (n *Node) FindSuccessor(ctx context.Context, id *internal.ID) (*internal.Node, error) {
	succ, err := n.findSuccessor(id.Id)
	if err != nil {
		return nil, err
	}

	if succ == nil {
		return nil, ERR_SUCCESSOR_NOT_FOUND
	}

	return succ, nil

}

// TODO
func (n *Node) ClosestPrecedingFinger(ctx context.Context, node *internal.ID) (*internal.Node, error) {
	return nil, nil
}

// TODO
func (n *Node) GetPredecessor(ctx context.Context, r *internal.ER) (*internal.Node, error) {
	n.predMtx.RLock()
	pred := n.predecessor
	n.predMtx.RUnlock()
	if pred == nil {
		return emptyNode, nil
	}
	return pred, nil
}

// TODO
func (n *Node) Notify(ctx context.Context, node *internal.Node) (*internal.ER, error) {
	n.predMtx.Lock()
	defer n.predMtx.Unlock()
	pred := n.predecessor
	if pred == nil || isBetween(node.Id, pred.Id, n.Id) {
		// fmt.Println("setting predecessor", n.Id, node.Id)
		n.predecessor = node
	}
	return emptyRequest, nil
}

// join allows this node to join an existing ring that a remote node
// is a part of (i.e., other).
func (n *Node) join(joinNode *internal.Node) error {
	// First check if node already present in the circle
	// Join this node to the same chord ring as parent
	var foo *internal.Node
	// // Ask if our id already exists on the ring.
	if joinNode != nil {
		remoteNode, err := n.findSuccessorRPC(joinNode, n.Id)
		if err != nil {
			return err
		}

		if idsEqual(remoteNode.Id, n.Id) {
			return ERR_NODE_ALREADY_EXISTS
		}
		foo = joinNode
		// fmt.Println("got sister", n.Id, foo.Id)
	} else {
		foo = n.Node
	}

	succ, err := n.findSuccessorRPC(foo, n.Id)
	if err != nil {
		return err
	}
	// fmt.Println("found succ for, ", n.Id, succ.Id)
	n.succMtx.Lock()
	n.successor = succ
	n.succMtx.Unlock()

	return nil
}

/*
Fig 5 implementation for find_succesor
First check if key present in local table, if not
then look for how to travel in the ring
*/
func (n *Node) findSuccessor(id []byte) (*internal.Node, error) {
	n.succMtx.RLock()
	defer n.succMtx.RUnlock()
	curr := n.Node
	succ := n.successor

	if succ == nil {
		return curr, nil
	}

	if isBetweenRightIncl(id, curr.Id, succ.Id) {
		// fmt.Println("1ad", n.Id, id, curr.Id, succ.Id)
		return succ, nil
	} else {
		//9ad [2] id:"\003" addr:"0.0.0.0:8002"  [3]
		pred := n.closestPrecedingNode(id)
		// fmt.Println("closest node ", n.Id, id, pred.Id)
		/*
			NOT SURE ABOUT THIS, RECHECK from paper!!!
			if preceeding node and current node are the same,
			store the key on this node
		*/
		if isEqual(pred.Id, n.Id) {
			return curr, nil
		}

		succ, err := n.findSuccessorRPC(pred, id)
		// fmt.Println("successor to closest node ", succ, err)
		if err != nil {
			return nil, err
		}
		if succ == nil {
			// not able to wrap around, current node is the successor
			return curr, nil
		}

	}
	return nil, nil
}

// Fig 5 implementation for closest_preceding_node
func (n *Node) closestPrecedingNode(id []byte) *internal.Node {
	n.predMtx.RLock()
	defer n.predMtx.RUnlock()

	curr := n.Node

	m := len(n.fingerTable) - 1
	for i := m; i >= 0; i-- {
		f := n.fingerTable[i]
		if f == nil || f.Node == nil {
			continue
		}
		if isBetween(f.Id, curr.Id, id) {
			return f.Node
		}
	}
	return curr
}

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getSuccessorRPC(node *internal.Node) (*internal.Node, error) {
	return n.transport.GetSuccessor(node)
}

// findSuccessorRPC finds the successor node of a given ID in the entire ring.
func (n *Node) findSuccessorRPC(node *internal.Node, id []byte) (*internal.Node, error) {
	return n.transport.FindSuccessor(node, id)
}

// getSuccessorRPC the successor ID of a remote node.
func (n *Node) getPredecessorRPC(node *internal.Node) (*internal.Node, error) {
	return n.transport.GetPredecessor(node)
}

// notifyRPC notifies a remote node that pred is its predecessor.
func (n *Node) notifyRPC(node, pred *internal.Node) error {
	return n.transport.Notify(node, pred)
}

func (n *Node) stabilize() {
	// fmt.Println("stabilize: ", n.Id, n.successor, n.predecessor)
	n.succMtx.RLock()
	succ := n.successor
	if succ == nil {
		n.succMtx.RUnlock()
		return
	}
	n.succMtx.RUnlock()

	x, err := n.getPredecessorRPC(succ)
	if err != nil || x == nil {
		fmt.Println("error getting predecessor, ", err, x)
		return
	}
	if x.Id != nil && isBetween(x.Id, n.Id, succ.Id) {
		n.succMtx.Lock()
		n.successor = x
		n.succMtx.Unlock()
		// fmt.Println("setting successor ", n.Id, x.Id)
	}
	n.notifyRPC(succ, n.Node)
}

func (n *Node) checkPredecessor() {
	x, err := n.getPredecessorRPC(n.Node)
	if err != nil || x == nil {
		fmt.Println("predecessor failed!")
		n.predMtx.Lock()
		n.predecessor = nil
		n.predMtx.Unlock()
	}
}
