package util

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestGetUnixSocketFile(t *testing.T) {

	sock, exists := GetUnixSocketFile("some")

	if exists {
		t.Errorf("unexpected %v", sock)
	}

	sock, _ = GetUnixSocketFile("localhost:8080")

	dir := os.TempDir()
	if strings.HasSuffix(dir, "/") {
		dir = strings.TrimSuffix(dir, "/")
	}

	if sock != fmt.Sprintf("%s/vasto.socket.8080", dir) {
		t.Errorf("unexpected socket: %v", sock)
	}

}
