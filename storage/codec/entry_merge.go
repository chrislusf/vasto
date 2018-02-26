package codec

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
)

func Merge(a, b []byte) (mergedBytes []byte, merged bool) {

	x, merged := MergeEntry(a, b)
	return x.ToBytes(), merged

}

func MergeEntry(a, b []byte) (mergedEntry *Entry, merged bool) {
	if a == nil {
		return FromBytes(b), true
	}

	x := FromBytes(a)

	merged = x.MergeWith(b)

	return x, merged

}

func (e *Entry) MergeWith(b []byte) (merged bool) {
	if b == nil {
		return true
	}

	y := FromBytes(b)

	switch y.OpAndDataType {
	case OpAndDataType(pb.OpAndDataType_BYTES):
		e.Value = append(e.Value, y.Value...)
	case OpAndDataType(pb.OpAndDataType_FLOAT64):
		result := util.BytesToFloat64(e.Value) + util.BytesToFloat64(y.Value)
		e.Value = util.Float64ToBytes(result)
	case OpAndDataType(pb.OpAndDataType_MAX_FLOAT64):
		left, right := util.BytesToFloat64(e.Value), util.BytesToFloat64(y.Value)
		if left < right {
			e.Value = y.Value
		}
	case OpAndDataType(pb.OpAndDataType_MIN_FLOAT64):
		left, right := util.BytesToFloat64(e.Value), util.BytesToFloat64(y.Value)
		if left > right {
			e.Value = y.Value
		}
	}

	return true

}
