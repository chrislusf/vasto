package change_log

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type logSegmentFile struct {
	fullName       string
	segment        uint16
	file           *os.File
	offset         int64
	sizeBuf        []byte
	followerCond   *sync.Cond
	logFileMaxSize int64
}

func newLogSegmentFile(fillName string, segment uint16, logFileMaxSize int64) *logSegmentFile {
	return &logSegmentFile{
		fullName:       fillName,
		segment:        segment,
		sizeBuf:        make([]byte, 4),
		followerCond:   &sync.Cond{L: &sync.Mutex{}},
		logFileMaxSize: logFileMaxSize,
	}
}

func (f *logSegmentFile) appendEntry(entry *LogEntry) (err error) {

	f.followerCond.L.Lock()
	defer f.followerCond.L.Unlock()

	data := entry.ToBytes()
	dataLen := len(data)
	binary.LittleEndian.PutUint32(f.sizeBuf, uint32(dataLen))
	_, err = f.file.WriteAt(f.sizeBuf, f.offset)
	if err != nil {
		return fmt.Errorf("write size info: %v", err)
	}
	_, err = f.file.WriteAt(data, f.offset+4)
	f.offset += int64(dataLen + 4)

	if err == nil {
		f.followerCond.Broadcast()
	}
	return err
}

/*
 * If offset is larger than latest entry, wait until new entry comes in.
 */
func (f *logSegmentFile) readEntries(offset int64, limit int) (entries []*LogEntry, nextOffset int64, err error) {

	if offset >= f.logFileMaxSize {
		return nil, 0, io.EOF
	}

	f.followerCond.L.Lock()
	for offset >= f.offset {
		f.followerCond.Wait()
	}
	f.followerCond.L.Unlock()

	nextOffset = offset

	for i := 0; i < limit; i++ {
		entry, next, err := f.readOneEntry(nextOffset)
		if err != nil {
			if i == 0 {
				return nil, 0, err
			}
			return entries, nextOffset, nil
		}
		entries = append(entries, entry)
		nextOffset = next
		if nextOffset >= f.logFileMaxSize {
			return entries, 0, io.EOF
		}
	}

	return

}

func (f *logSegmentFile) readOneEntry(offset int64) (entry *LogEntry, nextOffset int64, err error) {

	_, err = f.file.ReadAt(f.sizeBuf, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("read size info: %v", err)
	}
	dataLen := binary.LittleEndian.Uint32(f.sizeBuf)
	data := make([]byte, dataLen)
	_, err = f.file.ReadAt(data, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("read entry data: %v", err)
	}
	entry, err = FromBytes(data)
	return entry, offset + int64(dataLen+4), nil

}

func (f *logSegmentFile) isInitialized() bool {
	return f != nil && f.file != nil
}

func (f *logSegmentFile) open() error {
	if f.file != nil {
		f.file.Close()
		f.file = nil
	}
	if file, err := os.OpenFile(f.fullName, os.O_RDWR|os.O_CREATE, 0644); err != nil {
		return fmt.Errorf("open file %s: %v", f.fullName, err)
	} else {
		f.file = file
		if stat, err := f.file.Stat(); err != nil {
			return fmt.Errorf("stat file %s: %v", f.fullName, err)
		} else {
			f.offset = stat.Size()
		}
	}
	println("open log segment file", f.fullName)
	return nil
}

func (f *logSegmentFile) close() {
	println("close file", f.fullName)
	if f.file != nil {
		f.file.Close()
		f.file = nil
	}
	f.offset = 0
}

func (f *logSegmentFile) purge() {
	f.close()
	os.Remove(f.fullName)
	println("purge log segment file", f.fullName)
}
