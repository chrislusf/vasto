package gateway

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

func (gs *gatewayServer) testPut() error {
	grpcConnection, err := grpc.Dial("localhost:8280", grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	client := pb.NewVastoStoreClient(grpcConnection)

	ctx := context.Background()

	request := &pb.PutRequest{
		KeyValue: &pb.KeyValue{
			Key:   []byte("asdf"),
			Value: []byte("asdf"),
		},
		TimestampNs: 0,
		TtlMs:       0,
	}

	start := time.Now()
	N := int64(100000)

	fmt.Printf("%d grpc put test start\n", N)

	for i := int64(0); i < N; i++ {
		_, err = client.Put(ctx, request)
		if err != nil {
			log.Printf("put error: %v", err)
			return err
		}
	}

	taken := time.Now().Sub(start)
	fmt.Printf("%d put average taken %d ns/op\n", N, taken.Nanoseconds()/N)

	return nil
}
