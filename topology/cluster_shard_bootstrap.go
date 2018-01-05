package topology

import (
	"bytes"
	"fmt"
)

type BootstrapRequest struct {
	ServerId          int
	ShardId           int
	FromClusterSize   int
	ToClusterSize     int
	ReplicationFactor int
}

type BootstrapPlan struct {
	// 1. bootstrapSource the shard should read missing data from.
	//   If a list of bootstrap, pickBestBootstrapSource determines whether it should pick the best, or read all and filter
	// 2. transitionalFollowSource the shard should follow these shards, with its filter(always use the follower's filter)
	BootstrapSource          []ClusterShard
	PickBestBootstrapSource  bool
	TransitionalFollowSource []ClusterShard
	ToClusterSize            int

	IsNormalStart                bool
	IsNormalStartBootstrapNeeded bool
}

// BootstrapPeersWhenResize returns
func BootstrapPlanWithTopoChange(req *BootstrapRequest) (plan *BootstrapPlan) {
	plan = &BootstrapPlan{ToClusterSize: req.ToClusterSize}

	if req.FromClusterSize == req.ToClusterSize {
		plan.BootstrapSource = PartitionShards(req.ServerId, req.ShardId, req.ToClusterSize, req.ReplicationFactor)
		plan.PickBestBootstrapSource = true
		// this is for replicating shards on a different server
		plan.TransitionalFollowSource = []ClusterShard{{ShardId: req.ShardId, ServerId: req.ServerId}}
		return
	}

	if req.FromClusterSize < req.ToClusterSize {
		// growing cluster
		if req.ShardId >= req.FromClusterSize {
			// new shards just follow all existing shards, with its own filter
			for i := 0; i < req.FromClusterSize; i++ {
				plan.BootstrapSource = append(plan.BootstrapSource, ClusterShard{
					ShardId:  i,
					ServerId: i,
				})
			}
			plan.TransitionalFollowSource = plan.BootstrapSource
			return
		} else {
			// old shards
			if IsShardInLocal(req.ShardId, req.ServerId, req.ToClusterSize, req.ReplicationFactor) {
				if IsShardInLocal(req.ShardId, req.ServerId, req.FromClusterSize, req.ReplicationFactor) {
					// the shard does not move, no need to do anything
					return
				} else {
					// need to copy from a remote server, and no need for transitional follow
					// this can copy some unnecessariy data, should use the filter
					plan.BootstrapSource = PartitionShards(req.ServerId, req.ShardId, req.FromClusterSize, req.ReplicationFactor)
					plan.PickBestBootstrapSource = true
					return
				}
			} else {
				// not local in new cluster
				// moving out, nothing to do
				return
			}
		}
	} else {
		// shrinking cluster
		if req.ServerId >= req.ToClusterSize {
			// if retiring servers, nothing to bootstrap
			return
		}
		if req.ShardId >= req.ToClusterSize {
			// if retiring shards, nothing to bootstrap
			return
		}
		if IsShardInLocal(req.ShardId, req.ServerId, req.ToClusterSize, req.ReplicationFactor) {
			// local in new cluster
			if !IsShardInLocal(req.ShardId, req.ServerId, req.FromClusterSize, req.ReplicationFactor) {
				// the shard does not exist before the new cluster, need to copy existing one, and follow the to-be-retired shards with a filter
				plan.BootstrapSource = PartitionShards(req.ServerId, req.ShardId, req.FromClusterSize, req.ReplicationFactor)
				plan.PickBestBootstrapSource = true
				for i := req.ToClusterSize; i < req.FromClusterSize; i++ {
					plan.TransitionalFollowSource = append(plan.TransitionalFollowSource, ClusterShard{
						ShardId:  i,
						ServerId: i,
					})
				}
				return
			} else {
				// already exists, in both new and old cluster
				// add copying from the retiring servers
				for i := req.ToClusterSize; i < req.FromClusterSize; i++ {
					plan.BootstrapSource = append(plan.BootstrapSource, ClusterShard{
						ShardId:  i,
						ServerId: i,
					})
				}
				plan.TransitionalFollowSource = plan.BootstrapSource
				return
			}
		} else {
			// not local in new cluster
			// moving out, nothing to do
			return
		}
	}
}

func (plan *BootstrapPlan) String() string {
	var buf bytes.Buffer
	if plan.IsNormalStart {
		if plan.IsNormalStartBootstrapNeeded {
			buf.WriteString("check peer shards, ")
		}
		buf.WriteString("normal start")
	} else {
		if len(plan.BootstrapSource) > 0 {
			buf.WriteString("bootstraps from ")
			if plan.PickBestBootstrapSource {
				buf.WriteString("one of ")
			}
			buf.WriteString("[")
			for i := 0; i < len(plan.BootstrapSource); i++ {
				if i != 0 {
					buf.WriteString(",")
				}
				buf.WriteString(fmt.Sprintf("%d.%d", plan.BootstrapSource[i].ServerId, plan.BootstrapSource[i].ShardId))
			}
			buf.WriteString("] ")
		}

		if len(plan.TransitionalFollowSource) > 0 {
			buf.WriteString("temporarily follows [")
			for i := 0; i < len(plan.TransitionalFollowSource); i++ {
				if i != 0 {
					buf.WriteString(",")
				}
				buf.WriteString(fmt.Sprintf("%d.%d", plan.TransitionalFollowSource[i].ServerId, plan.TransitionalFollowSource[i].ShardId))
			}
			buf.WriteString("] ")
		}
		buf.WriteString("bootstrap start")
	}
	return buf.String()
}
