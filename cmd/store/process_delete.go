package store

import (
	"github.com/chrislusf/vasto/pb"
)

func (ss *storeServer) processDelete(deleteRequest *pb.DeleteRequest) *pb.DeleteResponse {

	resp := &pb.DeleteResponse{
		Ok: true,
	}
	err := ss.db.Delete(deleteRequest.Key)
	if err != nil {
		resp.Ok = false
		resp.Status = err.Error()
	}
	return resp

}
