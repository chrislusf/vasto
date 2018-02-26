package binlog

import (
	"fmt"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/vasto/pb"
	"io/ioutil"
	"math"
	"path"
	"strconv"
	"strings"
	"sync"
)

// LogManager manages the local binlogs
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
	hasShutdown  bool
}

const (
	constLogFilePrefix = "binlog-"
	constLogFileSuffix = ".dat"
)

// NewLogManager creates a new LogManager.
// logFileMaxSize is in bytes. Each log file's max size should be less than 2**48 = 32TB.
// There would be logFileCountLimit + 1 log files, with the latest one for writing.
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

// Initialze locates existing logs from disk, and creates files to write if needed.
func (m *LogManager) Initialze() error {

	if err := m.loadFilesFromDisk(); err != nil {
		return err
	}

	m.maybePrepareCurrentFileForWrite()

	return nil
}

// Shutdown stops current LogManager
func (m *LogManager) Shutdown() {

	m.followerCond.L.Lock()
	m.hasShutdown = true
	m.followerCond.Broadcast()
	m.followerCond.L.Unlock()

	m.filesLock.RLock()
	for _, file := range m.files {
		file.close()
	}
	m.filesLock.RUnlock()
}

// AppendEntry appends one log to the binlog file
func (m *LogManager) AppendEntry(entry *pb.LogEntry) error {
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

// ReadEntries reads a few entries from the binlog files, specified by the tuple of segment and offset.
func (m *LogManager) ReadEntries(segment uint32, offset int64,
	limit int) (entries []*pb.LogEntry, nextOffset int64, err error) {

	// wait until the new segment is ready
	m.followerCond.L.Lock()
	for !(segment <= m.segment && !m.hasShutdown) {
		// println("waiting on segment change, read entries segment1", segment, "offset", offset)
		m.followerCond.Wait()
	}
	m.followerCond.L.Unlock()

	if m.hasShutdown {
		return nil, 0, fmt.Errorf("%v shutdown in progress...", m.dir)
	}

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

// HasSegment checks whether the segment exists or not.
func (m *LogManager) HasSegment(segment uint32) bool {
	m.filesLock.Lock()

	_, ok := m.files[segment]

	m.filesLock.Unlock()

	return ok
}

// GetSegmentRange returns the inclusive range of start and stop of the segments.
func (m *LogManager) GetSegmentRange() (earlistSegment, latestSegment uint32) {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()

	if len(m.files) == 0 {
		return
	}

	earlistSegment = uint32(math.MaxInt32)

	for k := range m.files {
		if k <= earlistSegment {
			earlistSegment = k
		}
		if k > latestSegment {
			latestSegment = k
		}
	}

	return
}

func (m *LogManager) getFileName(segment uint32) string {
	return path.Join(m.dir, fmt.Sprintf(constLogFilePrefix+"%d"+constLogFileSuffix, segment))
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
		if strings.HasPrefix(name, constLogFilePrefix) && strings.HasSuffix(name, constLogFileSuffix) {
			segment := strings.TrimSuffix(strings.TrimPrefix(name, constLogFilePrefix), constLogFileSuffix)
			segmentNumber32, err := strconv.ParseUint(segment, 10, 32)
			segmentNumber := uint32(segmentNumber32)
			if err != nil {
				glog.Errorf("parse file name %s under %s", name, m.dir)
				return err
			}
			oneLogFile := newLogSegmentFile(m.getFileName(segmentNumber), segmentNumber, m.logFileMaxSize)
			// glog.V(2).Infof("add segment %d file %s", segmentNumber, oneLogFile.fullName)
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

// GetSegmentOffset returns the latest segment and offset.
func (m *LogManager) GetSegmentOffset() (uint32, int64) {
	if m.lastLogFile == nil {
		return m.segment, 0
	}
	return m.segment, m.lastLogFile.offset
}
