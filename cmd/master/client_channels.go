package master

import (
	"fmt"
	"strings"
	"sync"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

type clientChannels struct {
	sync.Mutex
	clientChans map[string]chan *pb.ClientMessage
}

func newClientChannels() *clientChannels {
	return &clientChannels{
		clientChans: make(map[string]chan *pb.ClientMessage),
	}
}

func (cc *clientChannels) addClient(keyspace keyspaceName, server serverAddress) (chan *pb.ClientMessage, error) {
	key := fmt.Sprintf("%s:%s", keyspace, server)
	cc.Lock()
	defer cc.Unlock()
	if _, ok := cc.clientChans[key]; ok {
		return nil, fmt.Errorf("client key is already in use: %s", key)
	}
	ch := make(chan *pb.ClientMessage, 3)
	cc.clientChans[key] = ch
	return ch, nil
}

func (cc *clientChannels) removeClient(keyspace keyspaceName, server serverAddress) error {
	key := fmt.Sprintf("%s:%s", keyspace, server)
	cc.Lock()
	defer cc.Unlock()

	ch, ok := cc.clientChans[key]
	if !ok {
		return fmt.Errorf("client key is not in use: %s", key)
	}

	delete(cc.clientChans, key)
	close(ch)
	return nil
}

func (cc *clientChannels) sendClient(keyspace keyspaceName, server serverAddress, msg *pb.ClientMessage) error {
	key := fmt.Sprintf("%s:%s", keyspace, server)
	cc.Lock()
	defer cc.Unlock()
	ch, ok := cc.clientChans[key]
	if !ok {
		return fmt.Errorf("client channel not found: %s", key)
	}
	ch <- msg
	return nil
}

func (cc *clientChannels) notifyClients(keyspace keyspaceName, msg *pb.ClientMessage) error {
	prefix := fmt.Sprintf("%s:", keyspace)
	cc.Lock()
	for key, ch := range cc.clientChans {
		if strings.HasPrefix(key, prefix) {
			ch <- msg
		}
	}
	cc.Unlock()
	return nil
}

func (cc *clientChannels) notifyStoreResourceUpdate(keyspace keyspaceName, nodes []*pb.ClusterNode, isDelete bool, isPromotion bool) error {
	return cc.notifyClients(
		keyspace,
		&pb.ClientMessage{
			Updates: &pb.ClientMessage_StoreResourceUpdate{
				Nodes:       nodes,
				IsDelete:    isDelete,
				Keyspace:    string(keyspace),
				IsPromotion: isPromotion,
			},
		},
	)
}

func (cc *clientChannels) sendClientCluster(keyspace keyspaceName, server serverAddress, cluster *topology.Cluster) error {
	return cc.sendClient(
		keyspace,
		server,
		&pb.ClientMessage{
			Cluster: cluster.ToCluster(),
		},
	)
}

func (cc *clientChannels) notifyClusterResize(keyspace keyspaceName, currentClusterSize, targetClusterSize uint32) error {
	return cc.notifyClients(
		keyspace,
		&pb.ClientMessage{
			Resize: &pb.ClientMessage_Resize{
				CurrentClusterSize: currentClusterSize,
				TargetClusterSize:  targetClusterSize,
				Keyspace:           string(keyspace),
			},
		},
	)
}
