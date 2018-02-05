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

	if !x.MergeWith(b) {
		return nil, false
	}

	return x, true

}

func (x *Entry) MergeWith(b []byte) (merged bool) {
	if b == nil {
		return true
	}

	y := FromBytes(b)

	if x.OpAndDataType != y.OpAndDataType {
		return false
	}

	switch x.OpAndDataType {
	case OpAndDataType(pb.OpAndDataType_BYTES):
		return false
	case OpAndDataType(pb.OpAndDataType_FLOAT64):
		result := util.BytesToFloat64(x.Value) + util.BytesToFloat64(y.Value)
		x.Value = util.Float64ToBytes(result)
	default:
		return false
	}

	return true

}
