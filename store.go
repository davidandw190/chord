package chord

import (
	"hash"

	internal "github.com/davidandw190/chord/internal"
)

// Storage represents the interface for key-value storage operations in the Chord DHT.
type Storage interface {
	Get(key string) ([]byte, error)
	Set(key, value string) error
	Delete(key string) error
	Between(from, to []byte) ([]*internal.KV, error)
	MDelete(keys ...string) error
}

// NewMapStore creates a new instance of Storage backed by an in-memory map.
func NewMapStore(hashFunc func() hash.Hash) Storage {
	return &mapStore{
		data: make(map[string]string),
		Hash: hashFunc,
	}
}

// mapStore is an implementation of Storage using an in-memory map.
type mapStore struct {
	data map[string]string
	Hash func() hash.Hash // Hash function to be used
}

// hashKey calculates the hash of the given key using the specified hash function.
func (ms *mapStore) hashKey(key string) ([]byte, error) {
	hasher := ms.Hash()
	if _, err := hasher.Write([]byte(key)); err != nil {
		return nil, err
	}
	hashedKey := hasher.Sum(nil)
	return hashedKey, nil
}

// Get retrieves the value associated with the given key from the in-memory map.
func (ms *mapStore) Get(key string) ([]byte, error) {
	value, found := ms.data[key]
	if !found {
		return nil, ERR_KEY_NOT_FOUND
	}
	return []byte(value), nil
}

// Set sets the value for the specified key in the in-memory map.
func (ms *mapStore) Set(key, value string) error {
	ms.data[key] = value
	return nil
}

// Delete removes the key-value pair associated with the given key from the in-memory map.
func (ms *mapStore) Delete(key string) error {
	delete(ms.data, key)
	return nil
}

// Between returns a list of key-value pairs from the in-memory map that fall within the specified key range (inclusive).
func (ms *mapStore) Between(from []byte, to []byte) ([]*internal.KV, error) {
	result := make([]*internal.KV, 0, 10)
	for k, v := range ms.data {
		hashedKey, err := ms.hashKey(k)
		if err != nil {
			continue
		}
		if isBetweenRightIncl(hashedKey, from, to) {
			pair := &internal.KV{
				Key:   k,
				Value: v,
			}
			result = append(result, pair)
		}
	}
	return result, nil
}

// MDelete deletes multiple keys from the in-memory map.
func (ms *mapStore) MDelete(keys ...string) error {
	for _, key := range keys {
		delete(ms.data, key)
	}
	return nil
}
