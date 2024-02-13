package chord

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"math/rand"
	"time"
)

var (
	ERR_SUCCESSOR_NOT_FOUND = errors.New("successor node could not be found")
	ERR_NODE_ALREADY_EXISTS = errors.New("node with the same ID already exists")
	ERR_KEY_NOT_FOUND       = errors.New("no key found")
)

// isEqual checks if two byte slices are equal.
func isEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// isPowerOfTwo checks if the given number is a power of two.
func isPowerOfTwo(num int) bool {
	return (num != 0) && ((num & (num - 1)) == 0)
}

// randStabilize generates a random duration within the specified range.
func randStabilize(min, max time.Duration) time.Duration {
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

// isBetweenRightIncl checks if the key is between a and b (right inclusive).
func isBetweenRightIncl(key, a, b []byte) bool {
	return isBetween(key, a, b) || bytes.Equal(key, b)
}

// isBetween checks if the key is strictly between two IDs exclusively.
func isBetween(key, a, b []byte) bool {
	switch bytes.Compare(a, b) {
	case 1:
		return bytes.Compare(a, key) == -1 || bytes.Compare(b, key) >= 0
	case -1:
		return bytes.Compare(a, key) == -1 && bytes.Compare(b, key) >= 0
	case 0:
		return bytes.Compare(a, key) != 0
	}
	return false
}

// FOR TESTS ONLY - calculates the SHA-1 hash of the given key and returns the result as a byte slice.
func GetHashID(key string) []byte {
	hasher := sha1.New()
	if _, err := hasher.Write([]byte(key)); err != nil {
		return nil
	}
	hashedKey := hasher.Sum(nil)
	return hashedKey
}
