package rocks

import (
	"bytes"

	"github.com/tecbot/gorocksdb"
)

// PrefixScan paginate through all entries with the prefix
// the first scan can have empty lastKey and limit = 0
func (d *rocks) PrefixScan(prefix, lastKey []byte, limit int, fn func(key, value []byte) bool) error {
	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetFillCache(false)
	defer opts.Destroy()
	iter := d.db.NewIterator(opts)
	defer iter.Close()
	return d.enumerate(iter, prefix, lastKey, limit, fn)
}

func (d *rocks) enumerate(iter *gorocksdb.Iterator, prefix, lastKey []byte, limit int, fn func(key, value []byte) bool) error {

	if len(lastKey) == 0 {
		iter.Seek(prefix)
	} else {
		iter.Seek(lastKey)
		iter.Next()
	}
	i := -1
	for ; iter.Valid(); iter.Next() {

		if limit > 0 {
			i++
			if i >= limit {
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
		return err
	}
	return nil
}
