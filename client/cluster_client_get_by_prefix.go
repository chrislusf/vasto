package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"google.golang.org/grpc"
)

func (c *ClusterClient) GetByPrefix(partitionKey, prefix []byte, limit uint32, lastSeenKey []byte, options ...topology.AccessOption) ([]*pb.KeyTypeValue, error) {

	prefixRequest := &pb.GetByPrefixRequest{
		Prefix:      prefix,
		Limit:       limit,
		LastSeenKey: lastSeenKey,
	}

	if partitionKey != nil {
		return c.sendToSingleShard(prefixRequest, partitionKey, options)
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

			responses, err := c.sendRequestsToOneShard("getByPrefix", []*pb.Request{{
				ShardId:     uint32(shardId),
				GetByPrefix: prefixRequest,
			}}, options)

			if err != nil {
				broadcastErr = err
				return
			}

			if len(responses) == 1 {
				for _, kv := range responses[0].GetByPrefix.KeyValues {
					chans[shardId] <- kv
				}
			}

		}()

	}

	return pb.LimitedMergeSorted(chans, int(prefixRequest.Limit)), broadcastErr

}

func (c *ClusterClient) sendToSingleShard(prefixRequest *pb.GetByPrefixRequest, partitionKey []byte, options []topology.AccessOption) (results []*pb.KeyTypeValue, err error) {

	shardId, _ := c.ClusterListener.GetShardId(c.keyspace, partitionKey)

	responses, err := c.sendRequestsToOneShard("getByPrefix", []*pb.Request{{
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
func (c *ClusterClient) sendRequestsToOneShard(name string, requests []*pb.Request, options []topology.AccessOption) (results []*pb.Response, err error) {

	if len(requests) == 0 {
		return nil, nil
	}

	cluster, err := c.GetCluster()
	if err != nil {
		return nil, err
	}

	shardId := requests[0].ShardId

	err = cluster.WithConnection(name, int(shardId), func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {

		conn, _, err := c.ClusterListener.GetConnectionByShardId(c.keyspace, int(shardId), options...)

		if err != nil {
			return err
		}

		responses, err := pb.SendRequests(conn, &pb.Requests{
			Keyspace: c.keyspace,
			Requests: requests,
		})
		conn.Close()

		if err != nil {
			return fmt.Errorf("shard %d process error: %v", shardId, err)
		}

		results = responses.Responses

		return nil
	})

	return

}
