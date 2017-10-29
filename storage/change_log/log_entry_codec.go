package change_log

func (e *LogEntry) ToBytes() []byte {

	b, _ := e.Marshal(nil)

	return b
}

func FromBytes(b []byte) (*LogEntry, error) {

	e := &LogEntry{}

	_, err := e.Unmarshal(b)

	return e, err
}
