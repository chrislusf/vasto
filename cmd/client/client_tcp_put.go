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

	bucketToKeyValues := make(map[int][]*Row)
	for _, row := range rows {
		bucket := cluster.FindBucket(row.PartitionHash)
		bucketToKeyValues[bucket] = append(bucketToKeyValues[bucket], row)
	}

	err := eachBucket(bucketToKeyValues, func(bucket int, rows []*Row) error {
		return c.batchPut(keyspace, bucket, rows, options...)
	})

	if err != nil {
		return fmt.Errorf("put error: %v", err)
	}

	return nil
}

func (c *VastoClient) batchPut(keyspace string, bucket int, rows []*Row, options ...topology.AccessOption) error {

	conn, replica, err := c.ClusterListener.GetConnectionByBucket(keyspace, bucket, options...)

	if err != nil {
		return err
	}

	requests := &pb.Requests{Keyspace: keyspace}

	for _, row := range rows {
		request := &pb.Request{
			Put: &pb.PutRequest{
				Replica: uint32(replica),
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

func eachBucket(buckets map[int][]*Row, eachFunc func(int, []*Row) error) (err error) {
	var wg sync.WaitGroup
	for bucket, rows := range buckets {
		wg.Add(1)
		go func(bucket int, rows []*Row) {
			defer wg.Done()
			if eachErr := eachFunc(bucket, rows); eachErr != nil {
				err = eachErr
			}
		}(bucket, rows)
	}
	wg.Wait()
	return
}
