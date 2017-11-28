package shell

import (
	"github.com/chrislusf/vasto/cmd/client"
)

type ShellOption struct {
	// fixed cluster mode options
	FixedCluster *string
	// dynamic cluster mode options
	Master     *string
	DataCenter *string
	Keyspace   *string
}

type shell struct {
	option *ShellOption

	vastoClient *client.VastoClient
}

func RunShell(option *ShellOption) {
	var b = &shell{
		option: option,
		vastoClient: client.New(
			&client.ClientOption{
				FixedCluster: option.FixedCluster,
				Master:       option.Master,
				DataCenter:   option.DataCenter,
				Keyspace:     option.Keyspace,
			},
		),
	}

	b.vastoClient.Start()

	b.runShell()

}
