package binlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type logSegmentFile struct {
	fullName        string
	segment         uint32
	file            *os.File
	offset          int64
	sizeBufForWrite []byte
	sizeBufForRead  []byte
	followerCond    *sync.Cond
	logFileMaxSize  int64
	hasShutdown     bool
	accessLock      sync.Mutex
}

func newLogSegmentFile(fillName string, segment uint32, logFileMaxSize int64) *logSegmentFile {
	return &logSegmentFile{
		fullName:        fillName,
		segment:         segment,
		sizeBufForWrite: make([]byte, 4),
		sizeBufForRead:  make([]byte, 4),
		followerCond:    &sync.Cond{L: &sync.Mutex{}},
		logFileMaxSize:  logFileMaxSize,
	}
}

func (f *logSegmentFile) appendEntry(entry *LogEntry) (err error) {

	f.accessLock.Lock()

	// println("append entry1", string(entry.ToBytesForWrite()))

	f.followerCond.L.Lock()

	// println("append entry2", string(entry.ToBytesForWrite()))

	data := entry.ToBytesForWrite()
	binary.LittleEndian.PutUint32(data, uint32(len(data)-4))
	writtenDataLen, err := f.file.WriteAt(data, f.offset)

	if err == nil && writtenDataLen == len(data) {
		// println("broadcast file condition change")
		f.followerCond.Broadcast()
		f.offset += int64(len(data))
	} else {
		log.Printf("append entry size %d, but %d: %v", len(data), writtenDataLen, err)
	}

	f.followerCond.L.Unlock()

	f.accessLock.Unlock()

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
	for offset >= f.offset && !f.hasShutdown {
		// println("readEntries offset", offset, f.offset)
		f.followerCond.Wait()
	}
	f.followerCond.L.Unlock()

	if f.hasShutdown {
		return nil, 0, fmt.Errorf("log file %v shutdown in progress...", f.fullName)
	}

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

	f.accessLock.Lock()
	defer f.accessLock.Unlock()

	sizeLen, err := f.file.ReadAt(f.sizeBufForRead, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("read size info: %v", err)
	}
	if sizeLen != 4 {
		return nil, 0, fmt.Errorf("read %d bytes for size info", sizeLen)
	}
	dataLen := binary.LittleEndian.Uint32(f.sizeBufForRead)
	data := make([]byte, dataLen)
	n, err := f.file.ReadAt(data, offset+4)
	if err != nil {
		println("reading", f.fullName, "offset", offset, "size", dataLen, err.Error())
		return nil, 0, fmt.Errorf("read entry data: %v", err)
	}
	if n != int(dataLen) {
		return nil, 0, fmt.Errorf("read wrong data size: %d, expecting %d", n, dataLen)
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
	log.Println("open log segment file", f.fullName, "to append")
	return nil
}

func (f *logSegmentFile) close() {

	f.followerCond.L.Lock()
	f.hasShutdown = true
	f.followerCond.Broadcast()
	f.followerCond.L.Unlock()

	if f.file != nil {
		f.file.Close()
		f.file = nil
	}
	f.offset = 0
}

func (f *logSegmentFile) purge() {
	f.close()
	os.Remove(f.fullName)
	log.Println("purge log segment file", f.fullName)
}
