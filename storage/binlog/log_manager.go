package binlog

import (
	"fmt"
	"io/ioutil"
	"log"
	"path"
	"strconv"
	"strings"
	"sync"
)

type LogManager struct {
	dir               string
	logFileMaxSize    int64
	logFileCountLimit int

	filesLock sync.RWMutex
	files     map[uint32]*logSegmentFile

	// current actively written log file
	lastLogFile  *logSegmentFile
	segment      uint32
	offset       int64
	followerCond *sync.Cond
}

const (
	LogFilePrefix = "binlog-"
	LogFileSuffix = ".dat"
)

/*
 * Each log file's max size should be less than 2**48 = 32TB.
 * There could be logFileCountLimit + 1 log files, with the latest one for writing.
 */
func NewLogManager(dir string, id int, logFileMaxSize int64, logFileCountLimit int) *LogManager {
	m := &LogManager{
		dir:               dir,
		logFileMaxSize:    logFileMaxSize,
		logFileCountLimit: logFileCountLimit,
		files:             make(map[uint32]*logSegmentFile),
		followerCond:      &sync.Cond{L: &sync.Mutex{}},
	}
	return m
}

func (m *LogManager) Initialze() error {

	if err := m.loadFilesFromDisk(); err != nil {
		return err
	}

	m.maybePrepareCurrentFileForWrite()

	return nil
}

func (m *LogManager) AppendEntry(entry *LogEntry) error {
	if m.lastLogFile.offset >= m.logFileMaxSize {
		m.lastLogFile.close()
		m.followerCond.L.Lock()
		m.segment++
		m.maybeRemoveOldFiles()
		m.lastLogFile = nil
		m.maybePrepareCurrentFileForWrite()
		// println("broadcast segment condition change")
		m.followerCond.Broadcast()
		m.followerCond.L.Unlock()
	}

	return m.lastLogFile.appendEntry(entry)

}

func (m *LogManager) ReadEntries(segment uint32, offset int64,
	limit int) (entries []*LogEntry, nextOffset int64, err error) {

	// wait until the new segment is ready
	m.followerCond.L.Lock()
	for !(segment <= m.segment) {
		// println("waiting on segment change, read entries segment1", segment, "offset", offset)
		m.followerCond.Wait()
	}
	m.followerCond.L.Unlock()

	m.filesLock.Lock()
	oneLogFile, ok := m.files[segment]
	m.filesLock.Unlock()

	if !ok {
		if segment == m.segment {
			oneLogFile = m.lastLogFile
		} else {
			return nil, 0, fmt.Errorf("already purged segment %d", segment)
		}
	}

	return oneLogFile.readEntries(offset, limit)
}

func (m *LogManager) maybeRemoveOldFiles() {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	for segment, oneLogFile := range m.files {
		if segment+uint32(m.logFileCountLimit) < m.segment {
			oneLogFile.purge()
			delete(m.files, segment)
		}
	}
}

func (m *LogManager) maybePrepareCurrentFileForWrite() (err error) {
	if m.lastLogFile == nil {
		m.lastLogFile = newLogSegmentFile(m.getFileName(m.segment), m.segment, m.logFileMaxSize)
		m.filesLock.Lock()
		defer m.filesLock.Unlock()
		m.files[m.segment] = m.lastLogFile
	}
	return m.lastLogFile.open()
}

func (m *LogManager) HasSegment(segment uint32) bool {
	m.filesLock.Lock()

	_, ok := m.files[segment]

	m.filesLock.Unlock()

	return ok
}

func (m *LogManager) GetSegmentRange() (earlistSegment, latestSegment uint32) {
	m.filesLock.Lock()

	for k, _ := range m.files {
		if k <= earlistSegment {
			earlistSegment = k
		}
		if k > latestSegment {
			latestSegment = k
		}
	}

	m.filesLock.Unlock()

	return
}

func (m *LogManager) getFileName(segment uint32) string {
	return path.Join(m.dir, fmt.Sprintf(LogFilePrefix+"%d"+LogFileSuffix, segment))
}

/*
Check existing log files.
*/
func (m *LogManager) loadFilesFromDisk() error {
	files, err := ioutil.ReadDir(m.dir)
	if err != nil {
		return err
	}

	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	maxSegmentNumber := uint32(0)
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, LogFilePrefix) && strings.HasSuffix(name, LogFileSuffix) {
			segment := strings.TrimSuffix(strings.TrimPrefix(name, LogFilePrefix), LogFileSuffix)
			segmentNumber32, err := strconv.ParseUint(segment, 10, 32)
			segmentNumber := uint32(segmentNumber32)
			if err != nil {
				log.Printf("parse file name %s under %s", name, m.dir)
				return err
			}
			oneLogFile := newLogSegmentFile(m.getFileName(segmentNumber), segmentNumber, m.logFileMaxSize)
			// log.Printf("add segment %d file %s", segmentNumber, oneLogFile.fullName)
			m.files[segmentNumber] = oneLogFile
			if maxSegmentNumber <= segmentNumber {
				maxSegmentNumber = segmentNumber
				m.lastLogFile = oneLogFile
			}
			// println(m.dir, "has file", oneLogFile.fullName)
		}
	}
	m.segment = maxSegmentNumber
	if m.lastLogFile != nil {
		// println(m.dir, "has the latest log file", m.lastLogFile.fullName)
	} else {
		// println(m.dir, "has no existing log files")
	}

	return nil
}

func (m *LogManager) GetSegmentOffset() (uint32, int64) {
	if m.lastLogFile == nil {
		return m.segment, 0
	}
	return m.segment, m.lastLogFile.offset
}
