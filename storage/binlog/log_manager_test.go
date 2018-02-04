package binlog

import (
	"fmt"
	"os"
	"testing"
	"github.com/chrislusf/vasto/pb"
)

func TestLogManager(t *testing.T) {

	m := NewLogManager(os.TempDir(), 2, 1024, 3)
	m.Initialze()

	for i := 0; i < 10; i++ {

		a := &pb.LogEntry{
			UpdatedAtNs: 2342342,
			Put: &pb.PutRequest{
				Key: []byte(fmt.Sprintf("key %4d", i)),
				PartitionHash: uint64(i),
				TtlSecond:     80908,
				OpAndDataType: pb.OpAndDataType_BYTES,
				Value:         []byte(fmt.Sprintf("value %4d", i)),
			},
		}

		m.AppendEntry(a)

	}

}
