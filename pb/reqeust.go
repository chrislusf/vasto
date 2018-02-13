package pb

import (
	"github.com/golang/glog"
)

func (r *Request) GetPartitionHash() (uint64) {
	if r.Get != nil {
		return r.Get.PartitionHash
	}
	if r.GetByPrefix != nil {
		// TODO change the caller function batchProcess to batchWriteProcess
		glog.Fatalf("unexpected r.GetByPrefix")
		// return r.GetByPrefix.PartitionHash
	}
	if r.Put != nil {
		return r.Put.PartitionHash
	}
	if r.Delete != nil {
		return r.Delete.PartitionHash
	}
	if r.Merge != nil {
		return r.Merge.PartitionHash
	}

	glog.Fatalf("unexpected request without partition hash %v", r)
	return 0
}
