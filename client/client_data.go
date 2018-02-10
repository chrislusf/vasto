package client

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

type Row struct {
	Key   *KeyObject
	Value []byte
}

func NewRow(key, value []byte) *Row {
	r := &Row{
		Key:   Key(key),
		Value: value,
	}
	return r
}
