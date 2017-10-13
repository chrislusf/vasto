package storage

import (
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

func newDB() *rocks {
	return &rocks{}
}

func (i *rocks) setup(path string) {
	i.path = path
	i.dbOptions = gorocksdb.NewDefaultOptions()
	i.dbOptions.SetCreateIfMissing(true)

	var err error
	i.db, err = gorocksdb.OpenDb(i.dbOptions, i.path)
	if err != nil {
		log.Fatal(err)
	}

	i.wo = gorocksdb.NewDefaultWriteOptions()
	//i.wo.DisableWAL(true)
	i.ro = gorocksdb.NewDefaultReadOptions()
}

func (i *rocks) Put(key []byte, msg []byte) error {
	return i.db.Put(i.wo, key, msg)
}

func (i *rocks) Get(key []byte) ([]byte, error) {
	return i.db.GetBytes(i.ro, key)
}

func (i *rocks) Delete(k []byte) error {
	return i.db.Delete(i.wo, k)
}

func (i *rocks) Destroy() {
	os.RemoveAll(i.path)
}

func (i *rocks) Close() {
	i.wo.Destroy()
	i.ro.Destroy()
	i.dbOptions.Destroy()
	i.db.Close()
}
