package client

import "github.com/chrislusf/vasto/util"

type keyObject struct {
	key           []byte
	partitionHash uint64
}

func Key(key []byte) *keyObject {
	return &keyObject{
		key:           key,
		partitionHash: util.Hash(key),
	}
}

func (k *keyObject) SetPartitionKey(partitionKey []byte) *keyObject {
	k.partitionHash = util.Hash(partitionKey)
	return k
}

func (k *keyObject) SetPartitionHash(partitionHash uint64) *keyObject {
	k.partitionHash = partitionHash
	return k
}

func (k *keyObject) GetKey() []byte {
	return k.key
}

func (k *keyObject) GetPartitionHash() uint64 {
	return k.partitionHash
}

type Row struct {
	Key   *keyObject
	Value []byte
}

func NewRow(key, value []byte) *Row {
	r := &Row{
		Key:   Key(key),
		Value: value,
	}
	return r
}
