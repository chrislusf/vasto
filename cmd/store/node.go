package store

import (
	"fmt"
	"github.com/chrislusf/vasto/storage/change_log"
	"github.com/chrislusf/vasto/storage/rocks"
	"github.com/chrislusf/vasto/topology/cluster_listener"
	"log"
	"os"
	"os/user"
	"strings"
)

type node struct {
	id              int
	db              *rocks.Rocks
	lm              *change_log.LogManager
	clusterListener *cluster_listener.ClusterListener

	// just to avoid repeatedly create these variables
	nextSegmentKey []byte
	nextOffsetKey  []byte
}

func newNodes(option *StoreOption, clusterListener *cluster_listener.ClusterListener) (nodes []*node, err error) {
	for i := 0; i < *option.ReplicationFactor; i++ {
		id := int(*option.Id) - i
		if id < 0 {
			id += clusterListener.ExpectedSize()
		}
		if i != 0 && id == int(*option.Id) {
			break
		}
		dir := fmt.Sprintf("%s/%d", *option.Dir, id)
		if strings.HasPrefix(dir, "~") {
			usr, err := user.Current()
			if err != nil {
				log.Fatal(err)
			}
			dir = usr.HomeDir + dir[1:]
		}
		err := os.MkdirAll(dir, 0755)
		if err != nil {
			return nil, fmt.Errorf("mkdir %s: %v", dir, err)
		}
		node := newNode(dir, id, clusterListener, *option.LogFileSizeMb, *option.LogFileCount)
		nodes = append(nodes, node)
		if i != 0 {
			go node.follow()
		}
	}
	return nodes, nil
}

func newNode(dir string, id int, clusterListener *cluster_listener.ClusterListener,
	logFileSizeMb int, logFileCount int) *node {
	n := &node{
		id:              id,
		db:              rocks.New(dir),
		clusterListener: clusterListener,
	}
	if logFileSizeMb > 0 {
		n.lm = change_log.NewLogManager(dir, id, int64(logFileSizeMb*1024*1024), logFileCount)
		n.lm.Initialze()
	}
	n.nextSegmentKey = []byte(fmt.Sprintf("%d.next.segment", n.id))
	n.nextOffsetKey = []byte(fmt.Sprintf("%d.next.offset", n.id))

	return n
}
