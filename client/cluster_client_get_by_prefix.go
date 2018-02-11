package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

func (c *ClusterClient) GetByPrefix(partitionKey, prefix []byte, limit uint32, lastSeenKey []byte, options ...topology.AccessOption) ([]*pb.KeyTypeValue, error) {

	prefixRequest := &pb.GetByPrefixRequest{
		Prefix:      prefix,
		Limit:       limit,
		LastSeenKey: lastSeenKey,
	}

	if partitionKey != nil {

		shardId, _ := c.ClusterListener.GetShardId(c.keyspace, partitionKey)

		return c.prefixQueryToSingleShard(shardId, prefixRequest, options)
	}

	return c.broadcastEachShard(prefixRequest, options)
}

func (c *ClusterClient) broadcastEachShard(prefixRequest *pb.GetByPrefixRequest, options []topology.AccessOption) (results []*pb.KeyTypeValue, broadcastErr error) {
	cluster, err := c.GetCluster()
	if err != nil {
		return nil, err
	}

	chans := make([]chan *pb.KeyTypeValue, cluster.ExpectedSize())

	for i := 0; i < cluster.ExpectedSize(); i++ {

		shardId := i
		chans[shardId] = make(chan *pb.KeyTypeValue)

		go func() {

			defer close(chans[shardId])

			keyValues, err := c.prefixQueryToSingleShard(shardId, prefixRequest, options)

			if err != nil {
				broadcastErr = err
				return
			}

			for _, kv := range keyValues {
				chans[shardId] <- kv
			}

		}()

	}

	return pb.LimitedMergeSorted(chans, int(prefixRequest.Limit)), broadcastErr

}

func (c *ClusterClient) prefixQueryToSingleShard(shardId int, prefixRequest *pb.GetByPrefixRequest, options []topology.AccessOption) (results []*pb.KeyTypeValue, err error) {

	responses, err := c.sendRequestsToOneShard([]*pb.Request{{
		ShardId:     uint32(shardId),
		GetByPrefix: prefixRequest,
	}}, options)

	if len(responses) == 1 {
		results = responses[0].GetByPrefix.KeyValues
	}

	return

}

// sendRequestsToOneShard send the requests to one shard
// assuming the requests going to the same shard
func (c *ClusterClient) sendRequestsToOneShard(requests []*pb.Request, options []topology.AccessOption) (results []*pb.Response, err error) {

	if len(requests) == 0 {
		return nil, nil
	}

	shardId := requests[0].ShardId

	conn, _, err := c.ClusterListener.GetConnectionByShardId(c.keyspace, int(shardId), options...)

	if err != nil {
		return nil, err
	}

	responses, err := pb.SendRequests(conn, &pb.Requests{
		Keyspace: c.keyspace,
		Requests: requests,
	})
	conn.Close()

	if err != nil {
		return nil, fmt.Errorf("shard %d process error: %v", shardId, err)
	}

	results = responses.Responses

	return

}
