package client

import (
	"fmt"

	"github.com/chrislusf/vasto/pb"
	"github.com/chrislusf/vasto/topology"
	"sync"
)

func (c *ClusterClient) Put(rows []*Row, options ...topology.AccessOption) error {

	if len(rows) == 0 {
		return nil
	}

	cluster, err := c.GetCluster()
	if err != nil {
		return err
	}

	shardIdToKeyValues := make(map[int][]*Row)
	for _, row := range rows {
		bucket := cluster.FindShardId(row.Key.GetPartitionHash())
		shardIdToKeyValues[bucket] = append(shardIdToKeyValues[bucket], row)
	}

	err = eachShard(shardIdToKeyValues, func(shardId int, rows []*Row) error {
		return c.batchPut(shardId, rows, options...)
	})

	if err != nil {
		return fmt.Errorf("put error: %v", err)
	}

	return nil
}

func (c *ClusterClient) batchPut(shardId int, rows []*Row, options ...topology.AccessOption) error {

	conn, _, err := c.ClusterListener.GetConnectionByShardId(c.keyspace, shardId, options...)

	if err != nil {
		return err
	}

	requests := &pb.Requests{Keyspace: c.keyspace}

	for _, row := range rows {
		request := &pb.Request{
			ShardId: uint32(shardId),
			Put: &pb.PutRequest{
				Key:           row.Key.GetKey(),
				PartitionHash: row.Key.GetPartitionHash(),
				TtlSecond:     0,
				OpAndDataType: pb.OpAndDataType_BYTES,
				Value:         row.Value,
			},
		}

		requests.Requests = append(requests.Requests, request)
	}

	_, err = pb.SendRequests(conn, requests)
	conn.Close()
	if err != nil {
		return fmt.Errorf("batch put error: %v", err)
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
