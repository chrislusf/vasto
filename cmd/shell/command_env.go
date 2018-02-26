package shell

import (
	"fmt"
	"strconv"

	"github.com/chrislusf/vasto/vs"
)

const (
	REPLICA = "REPLICA"
)

func parseEnv(client *vs.ClusterClient, env map[string]string) (err error) {
	if client == nil {
		return nil
	}
	for k, v := range env {
		if k == REPLICA {
			replica, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return fmt.Errorf("parsing %s=%s: %v", k, v, err)
			}
			if replica != 0 {
				client.Replica = int(replica)
			}
		}
	}
	return
}
