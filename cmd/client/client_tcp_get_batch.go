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

func (c *VastoClient) BatchGet(keys [][]byte, options ...topology.AccessOption) ([]*pb.KeyValue, error) {

	bucketToRequests := make(map[int][]*pb.Request)

	for _, key := range keys {
		bucket := c.clusterListener.FindBucket(util.Hash(key))
		if _, ok := bucketToRequests[bucket]; !ok {
			bucketToRequests[bucket] = make([]*pb.Request, 0, 4)
		}
		request := &pb.Request{
			Get: &pb.GetRequest{
				Key: key,
			},
		}
		bucketToRequests[bucket] = append(bucketToRequests[bucket], request)
	}

	outputChan := make(chan *answer, len(bucketToRequests))

	for bucket, requests := range bucketToRequests {
		go func(bucket int, requestList []*pb.Request) {

			conn, replica, err := c.clusterListener.GetConnectionByBucket(bucket, options...)
			if err != nil {
				outputChan <- &answer{err: err}
				return
			}
			defer conn.Close()

			for _, request := range requestList {
				request.Get.Replica = uint32(replica)
			}

			requests := &pb.Requests{}
			requests.Requests = requestList

			responses, err := pb.SendRequests(conn, requests)
			if err != nil {
				outputChan <- &answer{err: fmt.Errorf("batch get error: %v", err)}
				return
			}

			var output []*pb.KeyValue
			for _, response := range responses.Responses {
				output = append(output, response.Get.KeyValue)
			}

			outputChan <- &answer{keyvalues: output}

		}(bucket, requests)
	}

	var ret []*pb.KeyValue
	for i := 0; i < len(bucketToRequests); i++ {
		answer := <-outputChan
		if answer.err != nil {
			return nil, answer.err
		}
		ret = append(ret, answer.keyvalues...)
	}
	return ret, nil
}
