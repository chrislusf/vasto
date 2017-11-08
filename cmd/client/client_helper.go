package client

import (
	"gopkg.in/fatih/pool.v2"
	"net"
	"strings"
)

func ensureConnection(conn net.Conn, err error) {
	if err == nil {
		return
	}

	errStr := err.Error()
	if !(strings.Contains(errStr, "broken pipe") ||
		strings.Contains(errStr, "reset by peer") ||
		strings.Contains(errStr, "i/o timeout")) {

		if pc, ok := conn.(*pool.PoolConn); ok {
			pc.MarkUnusable()
			pc.Close()
		}

	}

}
