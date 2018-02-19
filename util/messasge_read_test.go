package util

import (
	"testing"
	"math/rand"
	"bytes"
)

func TestReadWriteMessage(t *testing.T) {
	data := make([]byte, 100)

	rand.Read(data)

	var buf bytes.Buffer

	WriteMessage(&buf, data)

	out, err := ReadMessage(&buf)
	if err != nil {
		t.Errorf("unexpeted: %v", err)
	}

	if bytes.Compare(data, out) != 0 {
		t.Errorf("unexpected data: %d", bytes.Compare(data, out))
	}

}
