package codec

import (
	"bytes"
	"fmt"
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/util"
	"testing"
	"time"
)

func TestPutRequestConversion(t *testing.T) {

	x := 123456789
	partitionHash := uint64(234234234)

	putRequest := &pb.PutRequest{
		Key:           []byte(fmt.Sprintf("k%d", x)),
		PartitionHash: partitionHash,
		TtlSecond:     0,
		OpAndDataType: pb.OpAndDataType_FLOAT64,
		Value:         util.Float64ToBytes(999),
	}

	putEntry := NewPutEntry(putRequest, uint64(time.Now().UnixNano()))

	putBytes := putEntry.ToBytes()

	putEntry2 := FromBytes(putBytes)

	if bytes.Compare(putEntry.Value, putEntry2.Value) != 0 {
		t.Errorf("codec value error: %x %x", putEntry.Value, putEntry2.Value)
	}

	if GetPartitionHashFromBytes(putBytes) != partitionHash {
		t.Errorf("codec partition hash error: %x %x", GetPartitionHashFromBytes(putBytes), partitionHash)
	}

	if putEntry2.IsExpired() {
		t.Error("codec isExpired error")
	}

}
