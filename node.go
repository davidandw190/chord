package chord

import (
	"hash"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type Config struct {
	id           []byte
	addr         string
	serverOpts   []grpc.ServerOption
	dialOpts     []grpc.DialOption
	HashFunc     func() hash.Hash // Hash function to use
	StabilizeMin time.Duration    // Minimum stabilization time
	StabilizeMax time.Duration    // Maximum stabilization time
	Timeout      time.Duration
	MaxIdle      time.Duration
}

type Storage interface {
	Get(string) string
	Put(string, string) error
}

type Node struct {
	*Node

	predecessor *Node
	predMtx     sync.RWMutex

	successor *Node
	succMtx   sync.RWMutex

	shutdownCh chan struct{}

	fingerTable fingerTable
	ftMtx       sync.RWMutex

	storage Storage
	stMtx   sync.RWMutex

	// transport ChordClient
	tsMtx sync.RWMutex

	lastStablized time.Time
}
