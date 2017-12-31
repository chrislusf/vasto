package topology

type AccessOption func(bucket int, ringSize int) (node, replica int)

func NewAccessOption(replica int) AccessOption {
	return func(serverId int, ringSize int) (int, int) {
		if serverId+replica >= ringSize {
			return serverId + replica - ringSize, replica
		}
		return serverId + replica, replica
	}
}
