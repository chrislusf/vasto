package util

import (
	"testing"
	"math/rand"
	"bytes"
	"encoding/binary"
)

func TestBufferedWriter(t *testing.T) {
	data := make([]byte, 100)

	rand.Read(data)

	var buf bytes.Buffer

	WriteMessage(&buf, data)

	out := buf.Bytes()

	dataLen := binary.LittleEndian.Uint32(out[0:4])

	if dataLen != 100 {
		t.Errorf("unexpected data length: %d", dataLen)
	}

	if bytes.Compare(data, out[4:]) != 0 {
		t.Errorf("unexpected data: %d", bytes.Compare(data, out[4:]))
	}

}
