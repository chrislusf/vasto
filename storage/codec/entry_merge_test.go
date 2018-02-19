package codec

import (
	"testing"
	"github.com/chrislusf/vasto/pb"
	"fmt"
	"github.com/chrislusf/vasto/util"
	"time"
)

func TestMergeFloat64(t *testing.T) {

	x := 123456789
	partitionHash := uint64(234234234)

	mergeRequest := &pb.MergeRequest{
		Key:           []byte(fmt.Sprintf("k%d", x)),
		PartitionHash: partitionHash,
		OpAndDataType: pb.OpAndDataType_FLOAT64,
		Value:         util.Float64ToBytes(999),
	}

	mergeEntry := NewMergeEntry(mergeRequest, uint64(time.Now().UnixNano()))

	mergeBytes1 := mergeEntry.ToBytes()

	mergeEntry.Value = util.Float64ToBytes(-1)

	mergeBytes2 := mergeEntry.ToBytes()

	mergedBytes, merged := Merge(mergeBytes1, mergeBytes2)

	if !merged {
		t.Error("merge error")
	}

	mergedEntry := FromBytes(mergedBytes)

	mergedValue := util.BytesToFloat64(mergedEntry.Value)

	if mergedValue != 998 {
		t.Errorf("merge error: %f %d", mergedValue, 998)
	}
}

func TestMergeMax(t *testing.T) {

	mergedBytes, merged := Merge((&Entry{
		OpAndDataType: OpAndDataType(pb.OpAndDataType_MAX_FLOAT64),
		Value:         util.Float64ToBytes(234),
	}).ToBytes(), (&Entry{
		OpAndDataType: OpAndDataType(pb.OpAndDataType_MAX_FLOAT64),
		Value:         util.Float64ToBytes(345),
	}).ToBytes())

	if !merged {
		t.Error("merge error")
	}

	mergedEntry := FromBytes(mergedBytes)

	mergedValue := util.BytesToFloat64(mergedEntry.Value)

	if mergedValue != 345 {
		t.Errorf("merge error: %f %d", mergedValue, 345)
	}
}

func TestMergeMin(t *testing.T) {

	mergedBytes, merged := Merge((&Entry{
		OpAndDataType: OpAndDataType(pb.OpAndDataType_MIN_FLOAT64),
		Value:         util.Float64ToBytes(345),
	}).ToBytes(), (&Entry{
		OpAndDataType: OpAndDataType(pb.OpAndDataType_MIN_FLOAT64),
		Value:         util.Float64ToBytes(234),
	}).ToBytes())

	if !merged {
		t.Error("merge error")
	}

	mergedEntry := FromBytes(mergedBytes)

	mergedValue := util.BytesToFloat64(mergedEntry.Value)

	if mergedValue != 234 {
		t.Errorf("merge error: %f %d", mergedValue, 234)
	}
}

func TestMergeBytes(t *testing.T) {

	mergedBytes, merged := Merge((&Entry{
		OpAndDataType: OpAndDataType(pb.OpAndDataType_BYTES),
		Value:         []byte("123"),
	}).ToBytes(), (&Entry{
		OpAndDataType: OpAndDataType(pb.OpAndDataType_BYTES),
		Value:         []byte("456"),
	}).ToBytes())

	if !merged {
		t.Error("merge error")
	}

	mergedEntry := FromBytes(mergedBytes)

	if string(mergedEntry.Value) != "123456" {
		t.Errorf("merge error: %x %x", mergedEntry.Value, "123456")
	}
}
