package vs

import (
	"github.com/chrislusf/vasto/pb"
)

// GetByPrefix list the entries keyed with the same prefix
// partitionKey: limit the prefix query to one specific shard.
// prefix: the entries should have a key with this prefix
// limit: number of entries to return
// lastSeenKey: the last key seen during pagination
func (c *ClusterClient) GetByPrefix(partitionKey, prefix []byte, limit uint32, lastSeenKey []byte) ([]*KeyValue, error) {

	prefixRequest := &pb.GetByPrefixRequest{
		Prefix:      prefix,
		Limit:       limit,
		LastSeenKey: lastSeenKey,
	}

	shardId, _ := c.ClusterListener.GetShardId(c.keyspace, partitionKey)
	return c.prefixQueryToSingleShard(shardId, prefixRequest)
}

// CollectByPrefix collects entries keyed by the prefix from all partitions
// prefix: the entries should have a key with this prefix
// limit: number of entries to return
// lastSeenKey: the last key seen during pagination
func (c *ClusterClient) CollectByPrefix(prefix []byte, limit uint32, lastSeenKey []byte) ([]*KeyValue, error) {

	prefixRequest := &pb.GetByPrefixRequest{
		Prefix:      prefix,
		Limit:       limit,
		LastSeenKey: lastSeenKey,
	}

	return c.broadcastEachShard(prefixRequest)
}

func (c *ClusterClient) broadcastEachShard(prefixRequest *pb.GetByPrefixRequest) (results []*KeyValue, broadcastErr error) {
	cluster, err := c.GetCluster()
	if err != nil {
		return nil, err
	}

	chans := make([]chan *KeyValue, 16*cluster.ExpectedSize())

	for i := 0; i < cluster.ExpectedSize(); i++ {

		shardId := i
		chans[shardId] = make(chan *KeyValue)

		go func() {

			defer close(chans[shardId])

			keyValues, err := c.prefixQueryToSingleShard(shardId, prefixRequest)

			if err != nil {
				broadcastErr = err
				return
			}

			for _, kv := range keyValues {
				chans[shardId] <- kv
			}

		}()

	}

	return limitedMergeSorted(chans, int(prefixRequest.Limit)), broadcastErr

}

func (c *ClusterClient) prefixQueryToSingleShard(shardId int, prefixRequest *pb.GetByPrefixRequest) (results []*KeyValue, err error) {

	responses, err := c.sendRequestsToOneShard(shardId, []*pb.Request{{
		ShardId:     uint32(shardId),
		GetByPrefix: prefixRequest,
	}})

	if len(responses) == 1 {
		for _, keyValue := range responses[0].GetByPrefix.KeyValues {
			kv := fromPbKeyTypeValue(keyValue)
			results = append(results, kv)
		}
	}

	return

}
