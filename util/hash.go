package util

import (
	"github.com/cespare/xxhash"
)

func Hash(key []byte) uint64 {
	return xxhash.Sum64(key)
}
