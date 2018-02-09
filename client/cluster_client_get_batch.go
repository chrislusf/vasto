package client

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
)

type answer struct {
	keyvalues []*pb.KeyTypeValue
	err       error
}

func (c *ClusterClient) BatchGet(keys [][]byte, options ...topology.AccessOption) (ret []*pb.KeyTypeValue, err error) {

	var requests []*pb.Request

	for _, key := range keys {
		request := &pb.Request{
			Get: &pb.GetRequest{
				Key:           key,
				PartitionHash: util.Hash(key),
			},
		}
		requests = append(requests, request)
	}

	outputChan := make(chan *answer, len(keys))
	go func() {
		err = c.batchProcess(requests, func(responses [] *pb.Response, err error) error {
			if err != nil {
				outputChan <- &answer{err: err}
				return nil
			}
			var output []*pb.KeyTypeValue
			for _, response := range responses {
				output = append(output, response.Get.KeyValue)
			}

			outputChan <- &answer{keyvalues: output}

			return nil
		})
		close(outputChan)
	}()

	for ans := range outputChan {
		if ans.err != nil {
			return nil, ans.err
		}
		ret = append(ret, ans.keyvalues...)
	}

	return ret, err
}
