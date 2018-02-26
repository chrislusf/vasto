package util

import (
	"github.com/cespare/xxhash"
)

// Hash calculates the hash value of the bytes
func Hash(key []byte) uint64 {
	return xxhash.Sum64(key)
}
