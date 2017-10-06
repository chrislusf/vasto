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
	grpcConnection, err := grpc.Dial("localhost:8279", grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConnection.Close()

	client := pb.NewVastoStoreClient(grpcConnection)

	ctx := context.Background()

	request := &pb.PutRequest{
		Key:         []byte("asdf"),
		Value:       []byte("asdf"),
		TimestampNs: 0,
		TtlMs:       0,
	}

	start := time.Now()
	N := int64(100000)

	for i := int64(0); i < N; i++ {
		_, err = client.Put(ctx, request)
		if err != nil {
			log.Printf("put error: %v", err)
			return err
		}
	}

	taken := time.Now().Sub(start)
	fmt.Printf("%d put average taken %d ns/op", N, taken.Nanoseconds()/N)

	return nil
}
