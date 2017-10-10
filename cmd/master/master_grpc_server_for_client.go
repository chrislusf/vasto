package master

import (
	"io"

	"fmt"
	"github.com/chrislusf/vasto/pb"
)

func (ms *masterServer) RegisterClient(stream pb.VastoMaster_RegisterClientServer) error {

	clientHeartbeat, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}

	fmt.Printf("client connected %v\n", clientHeartbeat.Location)

	ch, err := ms.clientChans.addClient(clientHeartbeat.Location)
	if err != nil {
		return err
	}

	clientDisconnectedChan := make(chan bool, 1)

	go func() {
		var e error
		for {
			_, e = stream.Recv()
			if e != nil {
				break
			}
		}
		ms.clientChans.removeClient(clientHeartbeat.Location)
		fmt.Printf("client disconnected %v: %v\n", clientHeartbeat.Location, e)
		clientDisconnectedChan <- true
	}()

	for {
		select {
		case msg := <-ch:
			fmt.Println("received message", msg)
			if err := stream.Send(msg); err != nil {
				return err
			}
		case <-clientDisconnectedChan:
			return nil
		}
	}

	return nil
}
