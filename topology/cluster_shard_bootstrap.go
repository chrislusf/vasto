package topology

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

	IsNormalStart                bool
	IsNormalStartBootstrapNeeded bool
}

// BootstrapPeersWhenResize returns
func BootstrapPlanWithTopoChange(req *BootstrapRequest) (plan *BootstrapPlan) {
	plan = &BootstrapPlan{}

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
			if isShardInLocal(req.ShardId, req.ServerId, req.ToClusterSize, req.ReplicationFactor) {
				if isShardInLocal(req.ShardId, req.ServerId, req.FromClusterSize, req.ReplicationFactor) {
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
		if isShardInLocal(req.ShardId, req.ServerId, req.ToClusterSize, req.ReplicationFactor) {
			// local in new cluster
			if !isShardInLocal(req.ShardId, req.ServerId, req.FromClusterSize, req.ReplicationFactor) {
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
