package vs

import (
	"github.com/chrislusf/vasto/util"
)

// KeyObject locates a entry, usually by a []byte as a key.
// Additionally, there could be multiple store partitions. A partition key or partition hash can be used to locate
// the store partitions. Prefix queries can be fairly fast if the entries shares the same partition.
type KeyObject struct {
	key           []byte
	partitionHash uint64
}

// Key creates a key object
func Key(key []byte) *KeyObject {
	return &KeyObject{
		key:           key,
		partitionHash: util.Hash(key),
	}
}

// SetPartitionKey sets the partition key, which hash value is used to route the key
// to the right partition.
func (k *KeyObject) SetPartitionKey(partitionKey []byte) *KeyObject {
	k.partitionHash = util.Hash(partitionKey)
	return k
}

// SetPartitionKey sets the partition hash to route the key to the right partition.
func (k *KeyObject) SetPartitionHash(partitionHash uint64) *KeyObject {
	k.partitionHash = partitionHash
	return k
}

// GetKey returns the key bytes
func (k *KeyObject) GetKey() []byte {
	return k.key
}

// GetPartitionHash returns the partition hash value
func (k *KeyObject) GetPartitionHash() uint64 {
	return k.partitionHash
}
