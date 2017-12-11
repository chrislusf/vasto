package topology

type AccessOption func(bucket int, ringSize int) (node, replica int)

func NewAccessOption(replica int) AccessOption {
	return func(bucket int, ringSize int) (int, int) {
		if bucket+replica >= ringSize {
			return bucket + replica - ringSize, replica
		}
		return bucket + replica, replica
	}
}
