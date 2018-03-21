package vs

import (
	"github.com/chrislusf/vasto/pb"
)

// KeyValue stores a reference to KeyObject, data type, and actual data bytes
type KeyValue struct {
	*KeyObject
	*ValueObject
}

// NewKeyValue creates a KeyValue with a bytes value
func NewKeyValue(key *KeyObject, value *ValueObject) *KeyValue {
	r := &KeyValue{
		KeyObject:   key,
		ValueObject: value,
	}
	return r
}

func fromPbKeyTypeValue(kv *pb.KeyTypeValue) *KeyValue {
	r := &KeyValue{
		KeyObject:   Key(kv.Key),
		ValueObject: Value(kv.Value, kv.DataType),
	}
	r.KeyObject.SetPartitionHash(kv.PartitionHash)
	return r
}
