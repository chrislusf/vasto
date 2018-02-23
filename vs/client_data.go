package vs

import "github.com/chrislusf/vasto/util"

type KeyObject struct {
	key           []byte
	partitionHash uint64
}

func Key(key []byte) *KeyObject {
	return &KeyObject{
		key:           key,
		partitionHash: util.Hash(key),
	}
}

func (k *KeyObject) SetPartitionKey(partitionKey []byte) *KeyObject {
	k.partitionHash = util.Hash(partitionKey)
	return k
}

func (k *KeyObject) SetPartitionHash(partitionHash uint64) *KeyObject {
	k.partitionHash = partitionHash
	return k
}

func (k *KeyObject) GetKey() []byte {
	return k.key
}

func (k *KeyObject) GetPartitionHash() uint64 {
	return k.partitionHash
}

type KeyBytesValue struct {
	Key   *KeyObject
	Value []byte
}

func NewKeyBytesValue(key, value []byte) *KeyBytesValue {
	r := &KeyBytesValue{
		Key:   Key(key),
		Value: value,
	}
	return r
}
