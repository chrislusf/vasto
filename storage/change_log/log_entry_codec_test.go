package change_log

import (
	"bytes"
	"testing"
)

func TestLogEntryCodec(t *testing.T) {

	a := &LogEntry{
		PartitionHash:      12314,
		UpdatedNanoSeconds: 2342342,
		TtlSecond:          80908,
		IsDelete:           false,
		Key:                []byte("this is the key"),
		Value:              []byte("this is the value"),
	}
	a.setCrc()

	b, _ := FromBytes(a.ToBytes())

	if a.PartitionHash != b.PartitionHash {
		t.Errorf("codec PartitionHash %v => %v", a.PartitionHash, b.PartitionHash)
	}
	if a.UpdatedNanoSeconds != b.UpdatedNanoSeconds {
		t.Errorf("codec UpdatedNanoSeconds %v => %v", a.UpdatedNanoSeconds, b.UpdatedNanoSeconds)
	}
	if a.IsDelete == false {
		if a.TtlSecond != b.TtlSecond {
			// this only works if a.IsDelete == false
			t.Errorf("codec TtlSecond %v => %v", a.TtlSecond, b.TtlSecond)
		}
	}
	if a.IsDelete != b.IsDelete {
		t.Errorf("codec IsDelete %v => %v", a.IsDelete, b.IsDelete)
	}
	if !bytes.Equal(a.Key, b.Key) {
		t.Errorf("codec Key %v => %v", a.Key, b.Key)
	}
	if !bytes.Equal(a.Value, b.Value) {
		t.Errorf("codec Value %v => %v", a.Value, b.Value)
	}

	a.IsDelete = !a.IsDelete
	b, _ = FromBytes(a.ToBytes())

	if a.IsDelete != b.IsDelete {
		t.Errorf("codec IsDelete2 %v => %v", a.IsDelete, b.IsDelete)
	}

	if !b.IsValid() {
		t.Errorf("Crc IsValid %v => %v", a.Crc, b.Crc)
	}
}

func BenchmarkEncodiing(b *testing.B) {

	b.StopTimer()

	a := &LogEntry{
		PartitionHash:      12314,
		UpdatedNanoSeconds: 2342342,
		TtlSecond:          80908,
		IsDelete:           true,
		Key:                []byte("this is the key"),
		Value:              []byte("this is the value"),
	}
	a.setCrc()

	b.ReportAllocs()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		a.ToBytes()
	}

}

func BenchmarkDecodiing(b *testing.B) {
	b.StopTimer()

	a := &LogEntry{
		PartitionHash:      12314,
		UpdatedNanoSeconds: 2342342,
		TtlSecond:          80908,
		IsDelete:           true,
		Crc:                23542345,
		Key:                []byte("this is the key"),
		Value:              []byte("this is the value"),
	}
	a.setCrc()

	data := a.ToBytes()

	b.ReportAllocs()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		FromBytes(data)
	}
}
