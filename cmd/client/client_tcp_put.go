package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"sync"
)

func (c *VastoClient) Put(keyspace string, rows []*Row, options ...topology.AccessOption) error {

	if len(rows) == 0 {
		return nil
	}

	cluster, found := c.ClusterListener.GetClusterRing(keyspace)
	if !found {
		return fmt.Errorf("no keyspace %s", keyspace)
	}

	shardIdToKeyValues := make(map[int][]*Row)
	for _, row := range rows {
		bucket := cluster.FindShardId(row.PartitionHash)
		shardIdToKeyValues[bucket] = append(shardIdToKeyValues[bucket], row)
	}

	err := eachShard(shardIdToKeyValues, func(shardId int, rows []*Row) error {
		return c.batchPut(keyspace, shardId, rows, options...)
	})

	if err != nil {
		return fmt.Errorf("put error: %v", err)
	}

	return nil
}

func (c *VastoClient) batchPut(keyspace string, shardId int, rows []*Row, options ...topology.AccessOption) error {

	conn, _, err := c.ClusterListener.GetConnectionByShardId(keyspace, shardId, options...)

	if err != nil {
		return err
	}

	requests := &pb.Requests{Keyspace: keyspace}

	for _, row := range rows {
		request := &pb.Request{
			ShardId: uint32(shardId),
			Put: &pb.PutRequest{
				KeyValue: &pb.KeyValue{
					Key:   row.Key,
					Value: row.Value,
				},
				PartitionHash: row.PartitionHash,
				TtlSecond:     0,
			},
		}

		requests.Requests = append(requests.Requests, request)
	}

	_, err = pb.SendRequests(conn, requests)
	conn.Close()
	if err != nil {
		return fmt.Errorf("put error: %v", err)
	}

	return nil
}

func eachShard(buckets map[int][]*Row, eachFunc func(int, []*Row) error) (err error) {
	var wg sync.WaitGroup
	for shardId, rows := range buckets {
		wg.Add(1)
		go func(shardId int, rows []*Row) {
			defer wg.Done()
			if eachErr := eachFunc(shardId, rows); eachErr != nil {
				err = eachErr
			}
		}(shardId, rows)
	}
	wg.Wait()
	return
}
