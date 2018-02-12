package binlog

import (
	"fmt"
	"os"
	"testing"
	"github.com/chrislusf/vasto/pb"
	"path"
)

func TestLogManager(t *testing.T) {

	dir := path.Join(os.TempDir(), "vasto_test")
	os.MkdirAll(dir, 0755)
	m := NewLogManager(dir, 2, 1024, 3)
	m.Initialze()

	for i := 0; i < 10; i++ {

		a := &pb.LogEntry{
			UpdatedAtNs: 2342342,
			Put: &pb.PutRequest{
				Key:           []byte(fmt.Sprintf("key %4d", i)),
				PartitionHash: uint64(i),
				TtlSecond:     80908,
				OpAndDataType: pb.OpAndDataType_BYTES,
				Value:         []byte(fmt.Sprintf("value %4d", i)),
			},
		}

		m.AppendEntry(a)

	}

	entries, nextOffset, err := m.ReadEntries(0, 0, 10)
	if err != nil {
		t.Errorf("read entries: %v", err)
	}
	println("next offset", nextOffset)

	for _, entry := range entries {
		println(string(entry.GetPut().GetKey()), ":", string(entry.GetPut().Value))
	}

	m.Shutdown()

	if len(entries) != 10 {
		t.Error("read entries failed")
	}

	os.RemoveAll(dir)

}
