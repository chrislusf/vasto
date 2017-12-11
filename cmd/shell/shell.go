package shell

import (
	"github.com/chrislusf/vasto/cmd/client"
	"context"
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
		vastoClient: client.NewClient(
			&client.ClientOption{
				FixedCluster: option.FixedCluster,
				Master:       option.Master,
				DataCenter:   option.DataCenter,
				Keyspace:     option.Keyspace,
			},
		),
	}

	b.vastoClient.StartClient(context.Background())

	b.vastoClient.ClusterListener.RegisterShardEventProcessor(b)

	b.runShell()

}
