package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

type answer struct {
	keyvalues []*pb.KeyValue
	err       error
}

func (c *ClusterClient) BatchGet(keys [][]byte, options ...topology.AccessOption) ([]*pb.KeyValue, error) {

	shardIdToRequests := make(map[int][]*pb.Request)

	r, err := c.GetCluster()
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		shardId := r.FindShardId(util.Hash(key))
		if _, ok := shardIdToRequests[shardId]; !ok {
			shardIdToRequests[shardId] = make([]*pb.Request, 0, 4)
		}
		request := &pb.Request{
			ShardId: uint32(shardId),
			Get: &pb.GetRequest{
				Key: key,
			},
		}
		shardIdToRequests[shardId] = append(shardIdToRequests[shardId], request)
	}

	outputChan := make(chan *answer, len(shardIdToRequests))

	for shardId, requests := range shardIdToRequests {
		go func(shardId int, requestList []*pb.Request) {

			conn, _, err := c.ClusterListener.GetConnectionByShardId(c.keyspace, shardId, options...)
			if err != nil {
				outputChan <- &answer{err: err}
				return
			}

			requests := &pb.Requests{Keyspace: c.keyspace}
			requests.Requests = requestList

			responses, err := pb.SendRequests(conn, requests)
			conn.Close()
			if err != nil {
				outputChan <- &answer{err: fmt.Errorf("batch get error: %v", err)}
				return
			}

			var output []*pb.KeyValue
			for _, response := range responses.Responses {
				output = append(output, response.Get.KeyValue)
			}

			outputChan <- &answer{keyvalues: output}

		}(shardId, requests)
	}

	var ret []*pb.KeyValue
	for i := 0; i < len(shardIdToRequests); i++ {
		answer := <-outputChan
		if answer.err != nil {
			return nil, answer.err
		}
		ret = append(ret, answer.keyvalues...)
	}
	return ret, nil
}
