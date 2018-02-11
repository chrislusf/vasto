package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"github.com/chrislusf/vasto/util"
	"google.golang.org/grpc"
)

func (c *ClusterClient) GetByPrefix(partitionKey, prefix []byte, limit uint32, lastSeenKey []byte, options ...topology.AccessOption) ([]*pb.KeyTypeValue, error) {

	prefixRequest := &pb.GetByPrefixRequest{
		Prefix:      prefix,
		Limit:       limit,
		LastSeenKey: lastSeenKey,
	}

	if partitionKey != nil {
		prefixRequest.PartitionHash = util.Hash(partitionKey)
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
		go cluster.WithConnection("getByPrefix", shardId, func(node *pb.ClusterNode, grpcConnection *grpc.ClientConn) error {
			defer close(chans[shardId])

			conn, _, err := c.ClusterListener.GetConnectionByShardId(c.keyspace, int(shardId), options...)

			if err != nil {
				broadcastErr = err
				return nil
			}

			responses, err := pb.SendRequests(conn, &pb.Requests{
				Keyspace: c.keyspace,
				Requests: []*pb.Request{&pb.Request{
					ShardId:     uint32(shardId),
					GetByPrefix: prefixRequest,
				}},
			})
			conn.Close()

			if err != nil {
				broadcastErr = fmt.Errorf("shard %d process error: %v", shardId, err)
				return nil
			}

			for _, response := range responses.Responses {
				for _, kv := range response.GetByPrefix.KeyValues {
					chans[shardId] <- kv
				}
			}
			return nil
		})

	}

	return pb.LimitedMergeSorted(chans, int(prefixRequest.Limit)), broadcastErr

}
