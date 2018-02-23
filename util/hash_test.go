package util

import (
	"fmt"
	"testing"
)

func TestHash(t *testing.T) {

	hash1 := Hash([]byte(fmt.Sprintf("k%d", 23445)))
	hash2 := Hash([]byte(fmt.Sprintf("k%d", 23446)))

	if hash1 == hash2 {
		t.Errorf("unexpected hash: %v %v", hash1, hash2)
	}

}
