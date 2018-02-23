package admin

import (
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"google.golang.org/grpc"
)

type AdminOption struct {
	Master *string
}

type administer struct {
	option       *AdminOption
	masterClient pb.VastoMasterClient
}

func RunAdmin(option *AdminOption) {

	conn, err := grpc.Dial(*option.Master, grpc.WithInsecure())
	if err != nil {
		glog.Fatalf("fail to dial %v: %v", *option.Master, err)
	}
	defer conn.Close()
	masterClient := pb.NewVastoMasterClient(conn)

	var a = &administer{
		option:       option,
		masterClient: masterClient,
	}

	a.runAdmin()

}
