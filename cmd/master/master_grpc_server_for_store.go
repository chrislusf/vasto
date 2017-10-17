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

	fmt.Printf("store connected %v\n", storeHeartbeat.Store.Address)

	node := topology.NewNodeFromStore(storeHeartbeat.Store)

	ms.Lock()
	ring, ok := ms.clusters[storeHeartbeat.DataCenter]
	if !ok {
		ring = topology.NewHashRing(storeHeartbeat.DataCenter)
		ms.clusters[storeHeartbeat.DataCenter] = ring
	}
	ms.Unlock()

	ring.Add(node)
	ms.clientChans.notifyStoreResourceUpdate(
		storeHeartbeat.DataCenter,
		[]*pb.StoreResource{
			storeHeartbeat.Store,
		},
		false,
	)

	storeDisconnectedChan := make(chan bool, 1)

	go func() {
		var e error
		for {
			_, e = stream.Recv()
			if e != nil {
				break
			}
		}
		fmt.Printf("store disconnected %v: %v\n", storeHeartbeat.Store.Address, e)
		storeDisconnectedChan <- true
	}()

	for {
		select {
		case <-storeDisconnectedChan:
			ring.Remove(node.GetId())
			ms.clientChans.notifyStoreResourceUpdate(
				storeHeartbeat.DataCenter,
				[]*pb.StoreResource{{
					Id:      int32(node.GetId()),
					Address: node.GetHost(),
				}},
				true,
			)
			return nil
		}
	}

	return nil
}
