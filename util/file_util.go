package util

import (
	"fmt"
	"os"
	"strings"
)

// FileExists checks whether the file exists
func FileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	return !os.IsNotExist(err)
}

// GetUnixSocketFile checks vasto unix socket exists corresponding to the tcp socket.
func GetUnixSocketFile(address string) (unixSocket string, fileExists bool) {
	localIp := GetLocalIP()
	if !(strings.HasPrefix(address, "localhost:") ||
		strings.HasPrefix(address, ":") ||
		strings.HasPrefix(address, localIp+":")) {
		return "", false
	}
	parts := strings.Split(address, ":")
	portString := parts[1]
	dir := os.TempDir()
	if strings.HasSuffix(dir, "/") {
		dir = strings.TrimSuffix(dir, "/")
	}
	unixSocket = fmt.Sprintf("%s/vasto.socket.%s", dir, portString)
	return unixSocket, FileExists(unixSocket)
}
