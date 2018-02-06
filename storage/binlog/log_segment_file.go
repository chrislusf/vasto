package binlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"github.com/chrislusf/vasto/pb"
	"github.com/golang/protobuf/proto"
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
	writeBuffer     *proto.Buffer
}

func newLogSegmentFile(fillName string, segment uint32, logFileMaxSize int64) *logSegmentFile {
	return &logSegmentFile{
		fullName:        fillName,
		segment:         segment,
		sizeBufForWrite: make([]byte, 4),
		sizeBufForRead:  make([]byte, 4),
		followerCond:    &sync.Cond{L: &sync.Mutex{}},
		logFileMaxSize:  logFileMaxSize,
		writeBuffer:     proto.NewBuffer(nil),
	}
}

func (f *logSegmentFile) appendEntry(entry *pb.LogEntry) (err error) {

	// lock writeBuffer, sizeBufForWrite, and file writes
	f.accessLock.Lock()

	// marshal the log entry
	f.writeBuffer.Reset()
	if err := f.writeBuffer.Marshal(entry); err != nil {
		f.accessLock.Unlock()
		return fmt.Errorf("appendEntry marshal log entry: %v", err)
	}

	// write to disk
	dataLen := len(f.writeBuffer.Bytes())
	binary.LittleEndian.PutUint32(f.sizeBufForWrite, uint32(dataLen))
	if _, err := f.file.WriteAt(f.sizeBufForWrite, f.offset); err != nil {
		f.accessLock.Unlock()
		return fmt.Errorf("appendEntry write log entry size: %v", err)
	}
	writtenDataLen, err := f.file.WriteAt(f.writeBuffer.Bytes(), f.offset+4)
	if err != nil {
		f.accessLock.Unlock()
		return fmt.Errorf("appendEntry write log entry data: %v", err)
	}
	f.accessLock.Unlock()

	if err == nil && writtenDataLen == dataLen {
		// println("broadcast file condition change")
		f.followerCond.L.Lock()
		f.offset += int64(dataLen)
		f.followerCond.Broadcast()
		f.followerCond.L.Unlock()
	} else {
		log.Printf("append entry size %d, but %d: %v", dataLen, writtenDataLen, err)
	}

	return err
}

/*
 * If offset is larger than latest entry, wait until new entry comes in.
 */
func (f *logSegmentFile) readEntries(offset int64, limit int) (entries []*pb.LogEntry, nextOffset int64, err error) {

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

func (f *logSegmentFile) readOneEntry(offset int64) (entry *pb.LogEntry, nextOffset int64, err error) {

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

	// unmarshal log entry
	entry = &pb.LogEntry{}
	if err := proto.Unmarshal(data, entry); err != nil {
		return nil, 0, fmt.Errorf("readOneEntry unmarshal: %v", err)
	}
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
