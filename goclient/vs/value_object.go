package vs

import (
	"github.com/chrislusf/vasto/util"
	"github.com/chrislusf/vasto/pb"
)

type ValueObject struct {
	valueType pb.OpAndDataType
	value     []byte
}

// Value creates a ValueObject with a specific type
func Value(value []byte, valueType pb.OpAndDataType) *ValueObject {
	return &ValueObject{
		value:     value,
		valueType: valueType,
	}
}

// BytesValue creates a ValueObject with a bytes value
func BytesValue(value []byte) *ValueObject {
	return &ValueObject{
		value:     value,
		valueType: pb.OpAndDataType_BYTES,
	}
}

// Float64Value creates a ValueObject with a float64 value
func Float64Value(value float64) *ValueObject {
	return &ValueObject{
		value:     util.Float64ToBytes(value),
		valueType: pb.OpAndDataType_FLOAT64,
	}
}

// GetValue returns the value bytes
func (v *ValueObject) GetValue() []byte {
	return v.value
}

// GetFloat64 returns the value float64
func (v *ValueObject) GetFloat64() float64 {
	return util.BytesToFloat64(v.value)
}

// GetValueType returns the data type of the value
func (v *ValueObject) GetValueType() pb.OpAndDataType {
	return v.valueType
}
