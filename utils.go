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

func isEqual(a, b []byte) bool {
	return bytes.Compare(a, b) == 0
}

func isPowerOfTwo(num int) bool {
	return (num != 0) && ((num & (num - 1)) == 0)
}

func randStabilize(min, max time.Duration) time.Duration {
	r := rand.Float64()
	return time.Duration((r * float64(max-min)) + float64(min))
}

// check if key is between a and b, right inclusive
func isBetweenRightIncl(key, a, b []byte) bool {
	return isBetween(key, a, b) || bytes.Equal(key, b)
}

// Checks if a key is STRICTLY between two ID's exclusively
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

// hashKey hashes a string to its appropriate size.
func hashKey(key string) ([]byte, error) {
	h := sha1.New()
	if _, err := h.Write([]byte(key)); err != nil {
		return nil, err
	}
	v := h.Sum(nil)

	return v[:8], nil
}
