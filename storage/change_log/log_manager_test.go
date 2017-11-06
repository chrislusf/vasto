package change_log

import (
	"fmt"
	"os"
	"testing"
)

func TestLogManager(t *testing.T) {

	m := NewLogManager(os.TempDir(), 2, 1024, 3)
	m.Initialze()

	for i := 0; i < 10; i++ {

		a := &LogEntry{
			PartitionHash:      uint64(i),
			UpdatedNanoSeconds: 2342342,
			TtlSecond:          80908,
			IsDelete:           false,
			Key:                []byte(fmt.Sprintf("key %4d", i)),
			Value:              []byte(fmt.Sprintf("value %4d", i)),
		}
		a.setCrc()

		m.AppendEntry(a)

	}

}
