package shell

import (
	"errors"
	"github.com/chrislusf/vasto/vs"
	"io"
)

type CommandEnv struct {
	env           map[string]string
	keyspace      string
	dataCenter    string
	clusterClient *vs.ClusterClient
}

type Command interface {
	Name() string
	Help() string
	Do(*vs.VastoClient, []string, *CommandEnv, io.Writer) error
}

var (
	commands           = []Command{}
	InvalidArguments   = errors.New("invalid arguments")
	NoKeyspaceSelected = errors.New("no keyspace selected")
)
