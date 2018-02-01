package pb

type writeRequest interface {
	GetPartitionHash() uint64
	GetKey() []byte
}

func (m *PutRequest) GetKey() []byte {
	if m != nil {
		return m.KeyValue.Key
	}
	return nil
}

func (entry *LogEntry) GetPartitionHash() uint64 {
	return entry.getWriteRequest().GetPartitionHash()
}

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
