package shell

import (
	"fmt"
	"strconv"

	"github.com/chrislusf/vasto/topology"
)

const (
	REPLICA = "REPLICA"
)

func parseEnv(env map[string]string) (option topology.AccessOption, err error) {
	for k, v := range env {
		if k == REPLICA {
			replica, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("parsing %s=%s: %v", k, v, err)
			}
			if replica != 0 {
				option = topology.NewAccessOption(int(replica))
			}
		}
	}
	return
}
