package vs

type WriteConfig struct {
	UpdatedAtNs uint64 // the update timestamp in nano seconds. Newer entries overwrite older ones. O means now.
	TtlSecond   uint32 // TTL in seconds. Updated_at + TTL determines the life of the entry. 0 means no TTL.
}

type AccessConfig struct {
	Replica int // control which replica instance to read from or write to. 0 means the primary copy.
}
