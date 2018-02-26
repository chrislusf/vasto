package pb

type writeRequest interface {
	GetPartitionHash() uint64
	GetKey() []byte
}

// GetPartitionHash returns the partition hash value
func (entry *LogEntry) GetPartitionHash() uint64 {
	return entry.getWriteRequest().GetPartitionHash()
}

// GetKey returns the key bytes
func (entry *LogEntry) GetKey() []byte {
	return entry.getWriteRequest().GetKey()
}

func (entry *LogEntry) getWriteRequest() (request writeRequest) {
	request = entry.GetPut()
	if request == nil {
		request = entry.GetDelete()
	}
	return request
}
