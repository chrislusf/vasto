package store

import (
	"github.com/chrislusf/gorocksdb"
	"github.com/chrislusf/vasto/storage/codec"
)

// NewVastoMergeOperator creates a MergeOperator object for rocksdb.
func NewVastoMergeOperator() gorocksdb.MergeOperator {
	return vastorMergeOperator{}
}

type vastorMergeOperator struct {
}

// Gives the client a way to express the read -> modify -> write semantics
// key:           The key that's associated with this merge operation.
//                Client could multiplex the merge operator based on it
//                if the key space is partitioned and different subspaces
//                refer to different types of data which have different
//                merge operation semantics.
// existingValue: null indicates that the key does not exist before this op.
// operands:      the sequence of merge operations to apply, front() first.
//
// Return true on success.
//
// All values passed in will be client-specific values. So if this method
// returns false, it is because client specified bad data or there was
// internal corruption. This will be treated as an error by the library.
func (mo vastorMergeOperator) FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool) {
	var entry *codec.Entry
	for i, operand := range operands {
		if i == 0 {
			var merged bool
			entry, merged = codec.MergeEntry(existingValue, operand)
			if !merged {
				return nil, false
			}
		} else {
			if !entry.MergeWith(operand) {
				return nil, false
			}
		}
	}
	return entry.ToBytes(), true
}

// This function performs merge(left_op, right_op)
// when both the operands are themselves merge operation types
// that you would have passed to a db.Merge() call in the same order
// (i.e.: db.Merge(key,left_op), followed by db.Merge(key,right_op)).
//
// PartialMerge should combine them into a single merge operation.
// The return value should be constructed such that a call to
// db.Merge(key, new_value) would yield the same result as a call
// to db.Merge(key, left_op) followed by db.Merge(key, right_op).
//
// If it is impossible or infeasible to combine the two operations, return false.
// The library will internally keep track of the operations, and apply them in the
// correct order once a base-value (a Put/Delete/End-of-Database) is seen.
func (mo vastorMergeOperator) PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool) {
	return codec.Merge(leftOperand, rightOperand)
}

// The name of the MergeOperator.
func (mo vastorMergeOperator) Name() string { return "VastorMergeOperator" }
