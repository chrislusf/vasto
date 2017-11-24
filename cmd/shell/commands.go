package shell

import (
	"errors"
	"github.com/chrislusf/vasto/cmd/client"
	"io"
)

type Command interface {
	Name() string
	Help() string
	Do([]string, map[string]string, io.Writer) error
	SetCilent(client *client.VastoClient)
}

var commands = []Command{}

var InvalidArguments = errors.New("Invalid Arguments")
