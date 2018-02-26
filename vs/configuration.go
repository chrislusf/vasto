package vs

type WriteConfig struct {
	UpdatedAtNs uint64
	TtlSecond   uint32
}

type AccessConfig struct {
	Replica int
}
