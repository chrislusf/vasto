package rocks

import (
	"fmt"
	"github.com/tecbot/gorocksdb"
	"io/ioutil"
	"os"
)

func (d *rocks) addSst(next func() (bool, []byte, []byte)) error {
	envOpts := gorocksdb.NewDefaultEnvOptions()
	defer envOpts.Destroy()
	opts := gorocksdb.NewDefaultOptions()
	defer opts.Destroy()
	w := gorocksdb.NewSSTFileWriter(envOpts, opts)
	defer w.Destroy()

	filePath, err := ioutil.TempFile("", "sst-file-")
	if err != nil {
		return fmt.Errorf("get temp file: %v", err)
	}
	defer func() {
		if err != nil {
			os.Remove(filePath.Name())
		}
	}()

	err = w.Open(filePath.Name())
	if err != nil {
		return fmt.Errorf("open temp file: %v", err)
	}

	var hasNext bool
	var key, value []byte
	for {
		hasNext, key, value = next()
		if !hasNext {
			break
		}
		err = w.Add(key, value)
		if err != nil {
			return fmt.Errorf("write sst file: %v", err)
		}
	}
	err = w.Finish()
	if err != nil {
		return fmt.Errorf("finish sst file: %v", err)
	}

	ingestOpts := gorocksdb.NewDefaultIngestExternalFileOptions()
	defer ingestOpts.Destroy()
	ingestOpts.SetMoveFiles(true)
	err = d.db.IngestExternalFile([]string{filePath.Name()}, ingestOpts)
	if err != nil {
		return fmt.Errorf("ingest sst file: %v", err)
	}

	return nil
}
