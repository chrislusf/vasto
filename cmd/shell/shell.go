package shell

import (
	"context"
	"github.com/chrislusf/vasto/vs"
)

type ShellOption struct {
	Master     *string
	DataCenter *string
	Keyspace   *string
	Verbose    *bool
}

type shell struct {
	option *ShellOption

	vastoClient *vs.VastoClient
}

func RunShell(option *ShellOption) {
	var b = &shell{
		option:      option,
		vastoClient: vs.NewVastoClient(context.Background(), "", *option.Master, *option.DataCenter),
	}

	if *option.Keyspace != "" {
		b.vastoClient.NewClusterClient(*option.Keyspace)
	}

	if *option.Verbose {
		// b.vastoClient.ClusterListener.RegisterShardEventProcessor(b)
		b.vastoClient.ClusterListener.SetVerboseLog(true)
	}

	b.runShell()

}
