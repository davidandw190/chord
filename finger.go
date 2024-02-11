package chord

import (
	"fmt"
	"math/big"

	internal "github.com/davidandw190/chord/internal"
)

// fingerTable represents a Chord node's finger table.
type fingerTable []*fingerEntry

// newFingerTable creates a new finger table for the given Chord node.
func newFingerTable(node *internal.Node) fingerTable {
	ft := make([]*fingerEntry, 8)
	for i := range ft {
		ft[i] = newFingerEntry(fingerMath(node.Id, i, 8), node)
	}
	return ft
}

// fingerEntry represents a single entry in the Chord node's finger table.
type fingerEntry struct {
	Id   []byte         // ID hash of (n + 2^i) mod (2^m)
	Node *internal.Node // RemoteNode that the finger points to
}

// newFingerEntry creates a new finger entry with the given ID and node.
func newFingerEntry(id []byte, node *internal.Node) *fingerEntry {
	return &fingerEntry{
		Id:   id,
		Node: node,
	}
}

// fingerMath performs the `(n + 2^i) mod (2^m)` operation needed to update finger table entries.
func fingerMath(n []byte, i int, m int) []byte {
	iInt := big.NewInt(2)
	iInt.Exp(iInt, big.NewInt(int64(i)), big.NewInt(100))
	mInt := big.NewInt(2)
	mInt.Exp(mInt, big.NewInt(int64(m)), big.NewInt(100))

	res := &big.Int{} // res will pretty much be an accumulator
	res.SetBytes(n).Add(res, iInt).Mod(res, mInt)

	return res.Bytes()
}

// fixFingers is called periodically to refresh finger table entries.
// next stores the index of the next finger to fix.
func (n *Node) fixFingers(next int) int {
	nextHash := fingerMath(n.Id, next, 8)
	succ, err := n.findSuccessor(nextHash)
	if err != nil || succ == nil {
		fmt.Println("finger lookup failed", n.Id, nextHash)
		// TODO: handle failed client here
		return (next + 1) % 8
	}

	finger := newFingerEntry(nextHash, succ)
	n.ftMtx.Lock()
	n.fingerTable[next] = finger
	n.ftMtx.Unlock()

	return (next + 1) % 8
}
