package change_log

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
)

type logFile struct {
	fullName string
	segment  uint16
	file     *os.File
}

type LogManager struct {
	dir               string
	logFileMaxSize    int64
	logFileCountLimit int

	filesLock sync.RWMutex
	files     map[uint16]*logFile

	lastLogFile *logFile
	segment     uint16
	offset      int64
}

const (
	LogFilePrefix = "log-"
	LogFileSuffix = ".dat"
)

/*
 * Each log file's max size should be less than 2**48 = 32TB.
 * There could be logFileCountLimit + 1 log files, with the latest one for writing.
 */
func NewLogManager(dir string, logFileMaxSize int64, logFileCountLimit int) *LogManager {
	m := &LogManager{
		dir:               dir,
		logFileMaxSize:    logFileMaxSize,
		logFileCountLimit: logFileCountLimit,
		files:             make(map[uint16]*logFile),
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

func (m *LogManager) AddEntry(entry *LogEntry) error {
	if m.offset >= m.logFileMaxSize {
		println("close file", m.lastLogFile.fullName)
		m.lastLogFile.file.Close()
		m.lastLogFile = nil
		m.segment++
		m.offset = 0
		m.maybeRemoveOldFiles()
		m.maybePrepareCurrentFileForWrite()
	}

	data := entry.ToBytes()
	m.lastLogFile.file.WriteAt(data, m.offset)
	m.offset += int64(len(data))

	return nil
}

func (m *LogManager) maybeRemoveOldFiles() {
	m.filesLock.Lock()
	defer m.filesLock.Unlock()
	for segment, oneLogFile := range m.files {
		if segment+uint16(m.logFileCountLimit) < m.segment {
			if oneLogFile.file != nil {
				oneLogFile.file.Close()
			}
			os.Remove(oneLogFile.fullName)
			delete(m.files, segment)
			println("purging file", oneLogFile.fullName)
		}
	}
}

func (m *LogManager) maybePrepareCurrentFileForWrite() error {
	if m.lastLogFile == nil {
		m.lastLogFile = &logFile{
			fullName: m.getFileName(m.segment),
			segment:  m.segment,
		}
	}
	if m.lastLogFile.file == nil {
		println("open file", m.lastLogFile.fullName)
		if file, err := os.OpenFile(m.lastLogFile.fullName, os.O_RDWR|os.O_CREATE, 0644); err != nil {
			return fmt.Errorf("Open file %s: %v", m.lastLogFile.fullName, err)
		} else {
			m.lastLogFile.file = file
			if stat, err := m.lastLogFile.file.Stat(); err != nil {
				return fmt.Errorf("Stat file %s: %v", m.lastLogFile.fullName, err)
			} else {
				m.offset = stat.Size()
			}
			m.lastLogFile.file.Seek(0, 2)
		}
	}
	return nil
}

func (m *LogManager) getFileName(segment uint16) string {
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
	maxSegmentNumber := uint16(0)
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, LogFilePrefix) && strings.HasSuffix(name, LogFileSuffix) {
			segment := strings.TrimSuffix(strings.TrimPrefix(name, LogFilePrefix), LogFileSuffix)
			segmentNumber16, err := strconv.ParseUint(segment, 10, 16)
			segmentNumber := uint16(segmentNumber16)
			if err != nil {
				log.Printf("Failed to parse %s", name)
				return err
			}
			oneLogFile := &logFile{
				fullName: m.getFileName(segmentNumber),
				segment:  segmentNumber,
			}
			m.files[segmentNumber] = oneLogFile
			if maxSegmentNumber < segmentNumber {
				maxSegmentNumber = segmentNumber
				m.lastLogFile = oneLogFile
			}
			println("loading log file", oneLogFile.fullName)
		}
	}
	m.segment = maxSegmentNumber
	if m.lastLogFile != nil {
		println("latest log file", m.lastLogFile.fullName)
	} else {
		println("no existing log files")
	}

	return nil
}
