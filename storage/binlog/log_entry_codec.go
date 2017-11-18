package binlog

func (e *LogEntry) ToBytesForWrite() []byte {

	buf := make([]byte, 4+e.Size())

	e.Marshal(buf[4:])

	return buf
}

func FromBytes(b []byte) (*LogEntry, error) {

	e := &LogEntry{}

	_, err := e.Unmarshal(b)

	return e, err
}
