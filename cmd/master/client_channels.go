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

func (cc *clientChannels) addClient(location *pb.Location) (chan *pb.ClientMessage, error) {
	key := fmt.Sprintf("%s:%s:%d", location.DataCenter, location.Server, location.Port)
	if _, ok := cc.clientChans[key]; ok {
		return nil, fmt.Errorf("client key is already in use: %s", key)
	}
	cc.Lock()
	ch := make(chan *pb.ClientMessage, 3)
	cc.clientChans[key] = ch
	cc.Unlock()
	return ch, nil
}

func (cc *clientChannels) removeClient(location *pb.Location) error {
	key := fmt.Sprintf("%s:%s:%d", location.DataCenter, location.Server, location.Port)
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
