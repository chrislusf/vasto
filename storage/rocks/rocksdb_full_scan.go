package rocks

import (
	"github.com/chrislusf/vasto/pb"
	"github.com/tecbot/gorocksdb"
)

// FullScan scan through all entries
func (d *Rocks) FullScan(batchSize int, fn func([]*pb.KeyValue) error) error {
	opts := gorocksdb.NewDefaultReadOptions()
	opts.SetFillCache(false)
	defer opts.Destroy()
	iter := d.db.NewIterator(opts)
	defer iter.Close()

	var rowCount int
	rows := make([]*pb.KeyValue, 0, batchSize)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {

		k := iter.Key()
		v := iter.Value()

		rowCount++
		rows = append(rows, &pb.KeyValue{
			Key:   k.Data(),
			Value: v.Data(),
		})
		k.Free()
		v.Free()

		if rowCount%batchSize == 0 {
			err := fn(rows)
			if err != nil {
				return err
			}
			rows = rows[:0]
		}

	}

	if len(rows) > 0 {
		err := fn(rows)
		if err != nil {
			return err
		}
	}

	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}
