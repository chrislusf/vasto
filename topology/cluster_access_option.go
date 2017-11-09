package topology

type AccessOption func(bucket int) (node, replica int)

func NewAccessOption(replica int) AccessOption {
	return func(bucket int) (int, int) {
		return bucket + replica, replica
	}
}
