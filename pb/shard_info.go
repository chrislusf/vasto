package pb

import "fmt"

func (s *ShardInfo) IdentifierOnThisServer() string {
	return fmt.Sprintf("%s:%d:%d", s.KeyspaceName, s.NodeId, s.ShardId)
}

func (s *ShardInfo) Clone() *ShardInfo {
	return &ShardInfo{
		KeyspaceName:      s.KeyspaceName,
		NodeId:            s.NodeId,
		ShardId:           s.ShardId,
		ClusterSize:       s.ClusterSize,
		ReplicationFactor: s.ReplicationFactor,
		IsCandidate:       s.IsCandidate,
		Status:            s.Status,
	}
}
