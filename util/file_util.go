package util

import (
	"fmt"
	"os"
	"strings"
)

func FileExists(filepath string) bool {
	_, err := os.Stat(filepath)
	return !os.IsNotExist(err)
}

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
