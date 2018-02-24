package vs

import (
	"github.com/chrislusf/vasto/util"
	"github.com/chrislusf/vasto/pb"
)

type KeyValue struct {
	*KeyObject
	valueType byte
	value     []byte
}

func NewKeyValue(key, value []byte) *KeyValue {
	r := &KeyValue{
		KeyObject: Key(key),
		valueType: byte(pb.OpAndDataType_BYTES),
		value:     value,
	}
	return r
}

func NewKeyFloat64Value(key []byte, value float64) *KeyValue {
	r := &KeyValue{
		KeyObject: Key(key),
		valueType: byte(pb.OpAndDataType_FLOAT64),
		value:     util.Float64ToBytes(value),
	}
	return r
}

func (kv *KeyValue) GetValue() []byte {
	return kv.value
}

func (kv *KeyValue) GetFloat64() float64 {
	if kv.valueType == byte(pb.OpAndDataType_FLOAT64) {
		return util.BytesToFloat64(kv.value)
	}
	return 0
}

func (kv *KeyValue) GetValueType() byte {
	return kv.valueType
}

func fromPbKeyTypeValue(kv *pb.KeyTypeValue) *KeyValue {
	r := &KeyValue{
		KeyObject: Key(kv.Key),
		valueType: byte(kv.DataType),
		value:     kv.Value,
	}
	r.KeyObject.SetPartitionHash(kv.PartitionHash)
	return r
}
