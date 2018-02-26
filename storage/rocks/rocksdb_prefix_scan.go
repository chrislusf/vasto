package rocks

import (
	"bytes"

	"fmt"
	"github.com/chrislusf/gorocksdb"
	"sync/atomic"
)

// PrefixScan paginate through all entries with the prefix
// the first scan can have empty lastKey and limit = 0
func (d *Rocks) PrefixScan(prefix, lastKey []byte, limit int, fn func(key, value []byte) bool) error {
	if newClientCounter := atomic.AddInt32(&d.clientCounter, 1); newClientCounter <= 0 {
		atomic.AddInt32(&d.clientCounter, -1)
		return ErrorShutdownInProgress
	}

	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetFillCache(false)
	iter := d.db.NewIterator(opts)

	err := d.enumerate(iter, prefix, lastKey, limit, fn)

	iter.Close()
	opts.Destroy()

	atomic.AddInt32(&d.clientCounter, -1)

	return err
}

func (d *Rocks) enumerate(iter *gorocksdb.Iterator, prefix, lastKey []byte, limit int, fn func(key, value []byte) bool) error {

	if len(lastKey) == 0 {
		iter.Seek(prefix)
	} else {
		iter.Seek(lastKey)

		k := iter.Key()
		key := k.Data()
		k.Free()

		if bytes.Equal(key, lastKey) {
			iter.Next()
		}
	}

	i := 0
	for ; iter.Valid(); iter.Next() {

		if limit > 0 {
			i++
			if i > limit {
				break
			}
		}

		k := iter.Key()
		key := k.Data()
		k.Free()

		if !bytes.HasPrefix(key, prefix) {
			break
		}

		v := iter.Value()
		ret := fn(key, v.Data())
		v.Free()

		if !ret {
			break
		}

	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("prefix scan iterator: %v", err)
	}
	return nil
}
