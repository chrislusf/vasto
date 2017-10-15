package rocks

import (
	"bytes"
	"log"
	"os"

	"github.com/tecbot/gorocksdb"
)

type rocks struct {
	path      string
	db        *gorocksdb.DB
	dbOptions *gorocksdb.Options
	wo        *gorocksdb.WriteOptions
	ro        *gorocksdb.ReadOptions
}

func New(path string) *rocks {
	r := &rocks{}
	r.setup(path)
	return r
}

func (d *rocks) setup(path string) {
	d.path = path
	d.dbOptions = gorocksdb.NewDefaultOptions()
	d.dbOptions.SetCreateIfMissing(true)
	// Required but not avaiable for now
	// d.dbOptions.SetAllowIngestBehind(true)

	var err error
	d.db, err = gorocksdb.OpenDb(d.dbOptions, d.path)
	if err != nil {
		log.Fatal(err)
	}

	d.wo = gorocksdb.NewDefaultWriteOptions()
	//d.wo.DisableWAL(true)
	d.ro = gorocksdb.NewDefaultReadOptions()
}

func (d *rocks) Put(key []byte, msg []byte) error {
	return d.db.Put(d.wo, key, msg)
}

func (d *rocks) Get(key []byte) ([]byte, error) {
	return d.db.GetBytes(d.ro, key)
}

func (d *rocks) Delete(k []byte) error {
	return d.db.Delete(d.wo, k)
}

func (d *rocks) Destroy() {
	os.RemoveAll(d.path)
}

func (d *rocks) Close() {
	d.wo.Destroy()
	d.ro.Destroy()
	d.dbOptions.Destroy()
	d.db.Close()
}

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
