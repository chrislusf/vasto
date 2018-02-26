package rocks

import (
	"os"

	"errors"
	"github.com/chrislusf/glog"
	"github.com/chrislusf/gorocksdb"
	"sync/atomic"
	"time"
)

// Rocks has options to run a local rocksdb instance
type Rocks struct {
	path             string
	db               *gorocksdb.DB
	dbOptions        *gorocksdb.Options
	wo               *gorocksdb.WriteOptions
	ro               *gorocksdb.ReadOptions
	compactionFilter *shardingCompactionFilter

	// used for locking
	clientCounter int32
}

var (
	ErrorShutdownInProgress = errors.New("shutdown in progress")
)

// NewDb creates a local rocksdb instance
func NewDb(path string, mergeOperator gorocksdb.MergeOperator) *Rocks {
	r := &Rocks{
		compactionFilter: &shardingCompactionFilter{},
	}
	r.setup(path, mergeOperator)
	return r
}

func (d *Rocks) setup(path string, mergeOperator gorocksdb.MergeOperator) {
	d.path = path
	d.dbOptions = gorocksdb.NewDefaultOptions()
	d.dbOptions.SetCreateIfMissing(true)
	d.dbOptions.SetAllowIngestBehind(true)
	// d.dbOptions.SetUseDirectReads(true)
	d.dbOptions.SetCompactionFilter(d.compactionFilter)
	if mergeOperator != nil {
		d.dbOptions.SetMergeOperator(mergeOperator)
	}
	// d.dbOptions.SetAllowConcurrentMemtableWrites()

	var err error
	d.db, err = gorocksdb.OpenDb(d.dbOptions, d.path)
	if err != nil {
		glog.Fatalf("open db at %s : %v", d.path, err)
	}

	d.wo = gorocksdb.NewDefaultWriteOptions()
	//d.wo.DisableWAL(true)
	d.ro = gorocksdb.NewDefaultReadOptions()
}

// Put puts to local rocksdb
func (d *Rocks) Put(key []byte, msg []byte) (err error) {
	// println("put", string(key), "value", string(msg))
	if newClientCounter := atomic.AddInt32(&d.clientCounter, 1); newClientCounter > 0 {
		err = d.db.Put(d.wo, key, msg)
	} else {
		err = ErrorShutdownInProgress
	}
	atomic.AddInt32(&d.clientCounter, -1)
	return
}

// Merge merges to local rocksdb
func (d *Rocks) Merge(key []byte, msg []byte) (err error) {
	// println("merge", string(key), "value", string(msg))
	if newClientCounter := atomic.AddInt32(&d.clientCounter, 1); newClientCounter > 0 {
		err = d.db.Merge(d.wo, key, msg)
	} else {
		err = ErrorShutdownInProgress
	}
	atomic.AddInt32(&d.clientCounter, -1)
	return
}

// Get gets from local rocksdb
func (d *Rocks) Get(key []byte) (data []byte, err error) {
	if newClientCounter := atomic.AddInt32(&d.clientCounter, 1); newClientCounter > 0 {
		data, err = d.db.GetBytes(d.ro, key)
	} else {
		err = ErrorShutdownInProgress
	}
	atomic.AddInt32(&d.clientCounter, -1)
	return
}

// Delete deletes from local rocksdb
func (d *Rocks) Delete(k []byte) (err error) {
	// println("del", string(k))
	if newClientCounter := atomic.AddInt32(&d.clientCounter, 1); newClientCounter > 0 {
		err = d.db.Delete(d.wo, k)
	} else {
		err = ErrorShutdownInProgress
	}
	atomic.AddInt32(&d.clientCounter, -1)
	return
}

// Destroy removes all data for the local rocksdb
func (d *Rocks) Destroy() {
	os.RemoveAll(d.path)
}

// Close shuts down local rocksdb
func (d *Rocks) Close() {
	for {
		swapped := atomic.CompareAndSwapInt32(&d.clientCounter, 0, -100)
		if swapped {
			break
		}
		glog.V(1).Infof("waiting to close db %s ...", d.path)
		time.Sleep(300 * time.Millisecond)
	}
	d.wo.Destroy()
	d.ro.Destroy()
	d.dbOptions.Destroy()
	d.db.Close()
	glog.V(1).Infof("closed db %s", d.path)
}

// LiveFilesSize returns the size of live files
func (d *Rocks) LiveFilesSize() (sum uint64) {
	for _, liveFileMetadata := range d.db.GetLiveFilesMetaData() {
		sum += uint64(liveFileMetadata.Size)
	}
	return sum
}

// GetLiveFilesMetaData returns the list of meta data of the live files
func (d *Rocks) GetLiveFilesMetaData() []gorocksdb.LiveFileMetadata {
	return d.db.GetLiveFilesMetaData()
}
