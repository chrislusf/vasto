package vs

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
)

type KeyValue struct {
	*KeyObject
	valueType pb.OpAndDataType
	value     []byte
}

func NewKeyValue(key, value []byte) *KeyValue {
	r := &KeyValue{
		KeyObject: Key(key),
		valueType: pb.OpAndDataType_BYTES,
		value:     value,
	}
	return r
}

func NewKeyFloat64Value(key []byte, value float64) *KeyValue {
	r := &KeyValue{
		KeyObject: Key(key),
		valueType: pb.OpAndDataType_FLOAT64,
		value:     util.Float64ToBytes(value),
	}
	return r
}

func (kv *KeyValue) GetValue() []byte {
	return kv.value
}

func (kv *KeyValue) GetFloat64() float64 {
	if kv.valueType == pb.OpAndDataType_FLOAT64 {
		return util.BytesToFloat64(kv.value)
	}
	return 0
}

func (kv *KeyValue) GetValueType() pb.OpAndDataType {
	return kv.valueType
}

func fromPbKeyTypeValue(kv *pb.KeyTypeValue) *KeyValue {
	r := &KeyValue{
		KeyObject: Key(kv.Key),
		valueType: kv.DataType,
		value:     kv.Value,
	}
	r.KeyObject.SetPartitionHash(kv.PartitionHash)
	return r
}
