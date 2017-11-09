package shell

import "github.com/chrislusf/vasto/cmd/client"

func init() {
	commands = append(commands, &CommandDelete{})
}

type CommandDelete struct {
	client *client.VastoClient
}

func (c *CommandDelete) Name() string {
	return "del"
}

func (c *CommandDelete) Help() string {
	return "key"
}

func (c *CommandDelete) SetCilent(client *client.VastoClient) {
	c.client = client
}

func (c *CommandDelete) Do(args []string, env map[string]string) (string, error) {
	options, err := parseEnv(env)
	if err != nil {
		return "", err
	}

	key := []byte(args[0])

	err = c.client.Delete(key, options...)

	return "", err
}
