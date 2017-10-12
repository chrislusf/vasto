package admin

import (
	"context"
	"fmt"
	"log"

	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
	"io"
)

type AdminOption struct {
	Master      *string
	DataCenter  *string
	ClusterSize *int32
}

type administer struct {
	option *AdminOption
}

func RunAdmin(option *AdminOption) {
	var a = &administer{
		option: option,
	}

	if *option.ClusterSize == 0 {
		a.list()
	} else {
		a.resizeCluster()
	}

}

func (b *administer) list() error {

	conn, err := grpc.Dial(*b.option.Master, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial %v: %v", *b.option.Master, err)
	}
	defer conn.Close()
	c := pb.NewVastoMasterClient(conn)

	listResponse, err := c.ListStores(
		context.Background(),
		&pb.ListRequest{
			DataCenter: *b.option.DataCenter,
		},
	)

	fmt.Printf("Cluster View Size: %d\n", listResponse.ClusterSize)

	for _, store := range listResponse.Stores {
		fmt.Printf("%4d: %32v\n", store.Id, store.Location.Address)
	}

	return nil
}

func (b *administer) resizeCluster() error {

	conn, err := grpc.Dial(*b.option.Master, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial %v: %v", *b.option.Master, err)
	}
	defer conn.Close()
	c := pb.NewVastoMasterClient(conn)

	stream, err := c.ResizeCluster(
		context.Background(),
		&pb.ResizeRequest{
			DataCenter:  *b.option.DataCenter,
			ClusterSize: uint32(*b.option.ClusterSize),
		},
	)

	for {
		resizeProgress, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("resize cluster error: %v", err)
		}
		if resizeProgress.Error != "" {
			log.Printf("Resize Error: %v", resizeProgress.Error)
			break
		}
		if resizeProgress.Progress != "" {
			log.Printf("Resize: %v", resizeProgress.Progress)
		}
	}

	return nil
}
