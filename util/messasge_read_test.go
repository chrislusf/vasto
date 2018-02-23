package util

import (
	"bytes"
	"encoding/binary"
	"github.com/magiconair/properties/assert"
	"io"
	"math/rand"
	"testing"
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

	_, err = ReadMessage(&buf)
	assert.Equal(t, err, io.EOF, "end of buffer")

	binary.Write(&buf, binary.LittleEndian, int32(0))
	out, err = ReadMessage(&buf)
	assert.Equal(t, err, nil, "zero length message")
	assert.Equal(t, len(out), 0, "zero length message")

	binary.Write(&buf, binary.LittleEndian, int32(1))
	out, err = ReadMessage(&buf)
	assert.Equal(t, err != nil, true, "wrong length message")

}
