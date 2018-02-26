package pb

import "fmt"

// IdentifierOnThisServer prints out the keyspace_name.server_id.shard_id
func (s *ShardInfo) IdentifierOnThisServer() string {
	return fmt.Sprintf("%s.%d.%d", s.KeyspaceName, s.ServerId, s.ShardId)
}

// Clone creates a new copy of ShardInfo
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
