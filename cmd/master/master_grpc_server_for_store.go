package master

import (
	"fmt"
	"io"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (ms *masterServer) RegisterStore(stream pb.VastoMaster_RegisterStoreServer) error {
	var storeHeartbeat *pb.StoreHeartbeat
	var err error

	storeHeartbeat, err = stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	fmt.Printf("store connected %v\n", storeHeartbeat.Store.Location)

	node := topology.NewNode(
		int(storeHeartbeat.Store.Id),
		fmt.Sprintf("%s:%d",
			storeHeartbeat.Store.Location.Server,
			storeHeartbeat.Store.Location.Port,
		),
	)

	ms.ring.Add(node)

	storeDisconnectedChan := make(chan bool, 1)

	go func() {
		var e error
		for {
			_, e = stream.Recv()
			if e != nil {
				break
			}
		}
		ms.ring.Remove(node)
		fmt.Printf("store disconnected %v: %v\n", storeHeartbeat.Store.Location, e)
		storeDisconnectedChan <- true
	}()

	ms.clientChans.notifyClients(
		storeHeartbeat.DataCenter,
		&pb.ClientMessage{
			Store: storeHeartbeat.Store,
		},
	)

	for {
		select {
		case <-storeDisconnectedChan:
			return nil
		}
	}

	return nil
}
