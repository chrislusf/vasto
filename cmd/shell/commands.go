package shell

import "github.com/chrislusf/vasto/cmd/client"

type Command interface {
	Name() string
	Help() string
	Do([]string) (string, error)
	SetCilent(client *client.VastoClient)
}

var commands = []Command{}
