package vs

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
)

// GetByPrefix list the entries keyed with the same prefix
// partitionKey: limit the prefix query to one specific shard.
// prefix: the entries should have a key with this prefix
// limit: number of entries to return
// lastSeenKey: the last key seen during pagination
func (c *ClusterClient) GetByPrefix(partitionKey, prefix []byte, limit uint32, lastSeenKey []byte, options ...topology.AccessOption) ([]*pb.KeyTypeValue, error) {

	prefixRequest := &pb.GetByPrefixRequest{
		Prefix:      prefix,
		Limit:       limit,
		LastSeenKey: lastSeenKey,
	}

	shardId, _ := c.ClusterListener.GetShardId(c.keyspace, partitionKey)
	return c.prefixQueryToSingleShard(shardId, prefixRequest, options)
}

// CollectByPrefix collects entries keyed by the prefix from all partitions
// prefix: the entries should have a key with this prefix
// limit: number of entries to return
// lastSeenKey: the last key seen during pagination
func (c *ClusterClient) CollectByPrefix(prefix []byte, limit uint32, lastSeenKey []byte, options ...topology.AccessOption) ([]*pb.KeyTypeValue, error) {

	prefixRequest := &pb.GetByPrefixRequest{
		Prefix:      prefix,
		Limit:       limit,
		LastSeenKey: lastSeenKey,
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
