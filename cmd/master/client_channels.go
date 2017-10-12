package master

import (
	"fmt"
	"sync"

	"github.com/chrislusf/vasto/pb"
	"strings"
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

func (cc *clientChannels) addClient(dataCenter, server string) (chan *pb.ClientMessage, error) {
	key := fmt.Sprintf("%s:%s", dataCenter, server)
	if _, ok := cc.clientChans[key]; ok {
		return nil, fmt.Errorf("client key is already in use: %s", key)
	}
	cc.Lock()
	ch := make(chan *pb.ClientMessage, 3)
	cc.clientChans[key] = ch
	cc.Unlock()
	return ch, nil
}

func (cc *clientChannels) removeClient(dataCenter, server string) error {
	key := fmt.Sprintf("%s:%s", dataCenter, server)
	cc.Lock()
	if ch, ok := cc.clientChans[key]; !ok {
		return fmt.Errorf("client key is not in use: %s", key)
	} else {
		delete(cc.clientChans, key)
		close(ch)
	}
	cc.Unlock()
	return nil
}

func (cc *clientChannels) notifyClients(dataCenter string, msg *pb.ClientMessage) error {
	prefix := dataCenter + ":"
	cc.Lock()
	for key, ch := range cc.clientChans {
		if strings.HasPrefix(key, prefix) {
			ch <- msg
		}
	}
	cc.Unlock()
	return nil
}

func (cc *clientChannels) notifyStoreResourceUpdate(dataCenter string, stores []*pb.StoreResource, isDelete bool) error {
	return cc.notifyClients(
		dataCenter,
		&pb.ClientMessage{
			Updates: &pb.ClientMessage_StoreResourceUpdate{
				Stores:   stores,
				IsDelete: isDelete,
			},
		},
	)
}

func (cc *clientChannels) notifyClusterSize(dataCenter string, currentClusterSize, newClusterSize uint32) error {
	return cc.notifyClients(
		dataCenter,
		&pb.ClientMessage{
			Resize: &pb.ClientMessage_Resize{
				CurrentClusterSize: currentClusterSize,
				NewClusterSize:     newClusterSize,
			},
		},
	)
}
