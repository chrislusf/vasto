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
	os.RemoveAll(dir)
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

func TestLargeLogManager(t *testing.T) {

	dir := path.Join(os.TempDir(), "vasto_test")
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)

	m := NewLogManager(dir, 2, 100, 2)
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

	m.Shutdown()

	m.Initialze()

	segment, _ := m.GetSegmentOffset()

	if !m.HasSegment(segment - 1) {
		t.Errorf("need to keep last 2 segments")
	}

	earlistSegment, latestSegment := m.GetSegmentRange()

	if latestSegment != segment {
		t.Errorf("last segment: %d %d", latestSegment, segment)
	}

	if earlistSegment != segment-2 {
		t.Errorf("earliest segment: %d %d", earlistSegment, segment-2)
	}

	m.Shutdown()

	// os.RemoveAll(dir)

}
