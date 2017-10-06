package util

import (
	"encoding/binary"
	"fmt"
	"io"
)

// ReadMessage reads out the []byte for one message
func ReadMessage(reader io.Reader) (m []byte, err error) {
	var length int32
	err = binary.Read(reader, binary.LittleEndian, &length)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message length: %v", err)
	}
	if length == 0 {
		return
	}
	m = make([]byte, length)
	var n int
	n, err = io.ReadFull(reader, m)
	if err == io.EOF {
		return nil, fmt.Errorf("Unexpected EOF when reading message size %d, but actual only %d.", length, n)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to read message content size %d, but read only %d: %v", length, n, err)
	}
	return m, nil
}
