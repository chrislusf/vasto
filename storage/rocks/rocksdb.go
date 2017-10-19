package rocks

import (
	"log"
	"os"

	"github.com/tecbot/gorocksdb"
)

type Rocks struct {
	path             string
	db               *gorocksdb.DB
	dbOptions        *gorocksdb.Options
	wo               *gorocksdb.WriteOptions
	ro               *gorocksdb.ReadOptions
	compactionFilter *shardingCompactionFilter
}

func New(path string) *Rocks {
	r := &Rocks{
		compactionFilter: &shardingCompactionFilter{},
	}
	r.setup(path)
	return r
}

func (d *Rocks) setup(path string) {
	d.path = path
	d.dbOptions = gorocksdb.NewDefaultOptions()
	d.dbOptions.SetCreateIfMissing(true)
	// TODO Required but not avaiable for now
	d.dbOptions.SetAllowIngestBehind(true)
	d.dbOptions.SetCompactionFilter(d.compactionFilter)

	var err error
	d.db, err = gorocksdb.OpenDb(d.dbOptions, d.path)
	if err != nil {
		log.Fatal(err)
	}

	d.wo = gorocksdb.NewDefaultWriteOptions()
	//d.wo.DisableWAL(true)
	d.ro = gorocksdb.NewDefaultReadOptions()
}

func (d *Rocks) Put(key []byte, msg []byte) error {
	return d.db.Put(d.wo, key, msg)
}

func (d *Rocks) Get(key []byte) ([]byte, error) {
	return d.db.GetBytes(d.ro, key)
}

func (d *Rocks) Delete(k []byte) error {
	return d.db.Delete(d.wo, k)
}

func (d *Rocks) Destroy() {
	os.RemoveAll(d.path)
}

func (d *Rocks) Close() {
	d.wo.Destroy()
	d.ro.Destroy()
	d.dbOptions.Destroy()
	d.db.Close()
}
