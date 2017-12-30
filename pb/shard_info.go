package pb

import "fmt"

func (s *ShardInfo) IdentifierOnThisServer() string {
	return fmt.Sprintf("%s:%d:%d", s.KeyspaceName, s.ServerId, s.ShardId)
}

func (s *ShardInfo) Clone() *ShardInfo {
	return &ShardInfo{
		KeyspaceName:      s.KeyspaceName,
		ServerId:          s.ServerId,
		ShardId:           s.ShardId,
		ClusterSize:       s.ClusterSize,
		ReplicationFactor: s.ReplicationFactor,
		IsCandidate:       s.IsCandidate,
		Status:            s.Status,
	}
}
