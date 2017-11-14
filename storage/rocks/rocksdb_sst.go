package rocks

import (
	"fmt"
	"github.com/tecbot/gorocksdb"
	"io/ioutil"
	"os"
)

func (d *Rocks) addSst(next func() (bool, []byte, []byte)) error {

	return d.AddSstByWriter(func(w *gorocksdb.SSTFileWriter) error {
		var hasNext bool
		var key, value []byte
		for {
			hasNext, key, value = next()
			if !hasNext {
				break
			}
			if err := w.Add(key, value); err != nil {
				return fmt.Errorf("write sst file: %v", err)
			}
		}
		return nil
	})

}

func (d *Rocks) AddSstByWriter(writerFunc func(*gorocksdb.SSTFileWriter) error) error {
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

	err = writerFunc(w)
	if err != nil {
		return fmt.Errorf("write: %v", err)
	}

	err = w.Finish()
	if err != nil {
		return fmt.Errorf("finish sst file: %v", err)
	}

	ingestOpts := gorocksdb.NewDefaultIngestExternalFileOptions()
	defer ingestOpts.Destroy()
	ingestOpts.SetMoveFiles(true)
	// TODO Required but not avaiable for now
	ingestOpts.SetIngestionBehind(true)
	err = d.db.IngestExternalFile([]string{filePath.Name()}, ingestOpts)
	if err != nil {
		return fmt.Errorf("ingest sst file: %v", err)
	}

	return nil
}
