package pb

import "fmt"

func (s *ShardStatus) IdentifierOnThisServer() string {
	return  fmt.Sprintf("%s:%d:%d", s.KeyspaceName, s.NodeId, s.ShardId)
}
