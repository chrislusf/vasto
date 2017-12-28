package client

import "github.com/chrislusf/vasto/util"

type Row struct {
	Key           []byte
	Value         []byte
	PartitionKey  []byte
	PartitionHash uint64
}

func NewRow(key, value []byte) *Row {
	r := &Row{
		Key:   key,
		Value: value,
	}
	r.SetPartitionKey(key)
	return r
}

func (r *Row) SetPartitionKey(partitionKey []byte) *Row {
	r.PartitionKey = partitionKey
	r.PartitionHash = util.Hash(r.PartitionKey)
	return r
}
