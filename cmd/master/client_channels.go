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

func (cc *clientChannels) addClient(keyspace keyspace_name, dataCenter data_center_name, server server_address) (chan *pb.ClientMessage, error) {
	key := fmt.Sprintf("%s:%s:%s", keyspace, dataCenter, server)
	cc.Lock()
	defer cc.Unlock()
	if _, ok := cc.clientChans[key]; ok {
		return nil, fmt.Errorf("client key is already in use: %s", key)
	}
	ch := make(chan *pb.ClientMessage, 3)
	cc.clientChans[key] = ch
	return ch, nil
}

func (cc *clientChannels) removeClient(keyspace keyspace_name, dataCenter data_center_name, server server_address) error {
	key := fmt.Sprintf("%s:%s:%s", keyspace, dataCenter, server)
	cc.Lock()
	defer cc.Unlock()
	if ch, ok := cc.clientChans[key]; !ok {
		return fmt.Errorf("client key is not in use: %s", key)
	} else {
		delete(cc.clientChans, key)
		close(ch)
	}
	return nil
}

func (cc *clientChannels) sendClient(keyspace keyspace_name, dataCenter data_center_name, server server_address, msg *pb.ClientMessage) error {
	key := fmt.Sprintf("%s:%s:%s", keyspace, dataCenter, server)
	cc.Lock()
	defer cc.Unlock()
	ch, ok := cc.clientChans[key]
	if !ok {
		return fmt.Errorf("client channel not found: %s", key)
	}
	ch <- msg
	return nil
}

func (cc *clientChannels) notifyClients(keyspace keyspace_name, dataCenter data_center_name, msg *pb.ClientMessage) error {
	prefix := fmt.Sprintf("%s:%s:", keyspace, dataCenter)
	cc.Lock()
	for key, ch := range cc.clientChans {
		if strings.HasPrefix(key, prefix) {
			ch <- msg
		}
	}
	cc.Unlock()
	return nil
}

func (cc *clientChannels) notifyStoreResourceUpdate(keyspace keyspace_name, dataCenter data_center_name, nodes []*pb.ClusterNode, isDelete bool, isPromotion bool) error {
	return cc.notifyClients(
		keyspace,
		dataCenter,
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

func (cc *clientChannels) sendClientCluster(keyspace keyspace_name, dataCenter data_center_name, server server_address, cluster *topology.Cluster) error {
	return cc.sendClient(
		keyspace,
		dataCenter,
		server,
		&pb.ClientMessage{
			Cluster: cluster.ToCluster(),
		},
	)
}

func (cc *clientChannels) notifyClusterSize(keyspace keyspace_name, dataCenter data_center_name, currentClusterSize, nextClusterSize uint32) error {
	return cc.notifyClients(
		keyspace,
		dataCenter,
		&pb.ClientMessage{
			Resize: &pb.ClientMessage_Resize{
				CurrentClusterSize: currentClusterSize,
				NextClusterSize:    nextClusterSize,
				Keyspace:           string(keyspace),
			},
		},
	)
}
