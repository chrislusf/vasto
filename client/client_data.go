package client

import "github.com/chrislusf/vasto/util"

type keyObject struct {
	key          []byte
	partitionKey []byte
}

func Key(key []byte) *keyObject {
	return &keyObject{
		key: key,
	}
}

func (k *keyObject) SetPartitionKey(partitionKey []byte) *keyObject {
	k.partitionKey = partitionKey
	return k
}

func (k *keyObject) GetKey() []byte {
	return k.key
}

func (k *keyObject) GetPartitionHash() uint64 {
	partitionKey := k.partitionKey
	if partitionKey == nil {
		partitionKey = k.key
	}
	return util.Hash(partitionKey)
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
