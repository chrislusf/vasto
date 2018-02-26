package shell

import (
	"errors"
	"github.com/chrislusf/vasto/vs"
	"io"
)

type commandEnv struct {
	env           map[string]string
	keyspace      string
	dataCenter    string
	clusterClient *vs.ClusterClient
}

type command interface {
	Name() string
	Help() string
	Do(*vs.VastoClient, []string, *commandEnv, io.Writer) error
}

var (
	commands              = []command{}
	errInvalidArguments   = errors.New("invalid arguments")
	errNoKeyspaceSelected = errors.New("no keyspace selected")
)
