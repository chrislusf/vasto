package shell

import "github.com/chrislusf/vasto/cmd/client"

func init() {
	commands = append(commands, &CommandPut{})
}

type CommandPut struct {
	client *client.VastoClient
}

func (c *CommandPut) Name() string {
	return "put"
}

func (c *CommandPut) Help() string {
	return "key value"
}

func (c *CommandPut) SetCilent(client *client.VastoClient) {
	c.client = client
}

func (c *CommandPut) Do(args []string) (string, error) {
	key := []byte(args[0])
	value := []byte(args[1])

	err := c.client.Put(key, value)

	return "", err
}
