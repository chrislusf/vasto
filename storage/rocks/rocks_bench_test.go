package rocks

import (
	"math/rand"
	"testing"
)

func BenchmarkPutByte4(b *testing.B) {
	benchPutWithSize(b, 4)
}

func BenchmarkPutByte16(b *testing.B) {
	benchPutWithSize(b, 16)
}

func BenchmarkPutByte256(b *testing.B) {
	benchPutWithSize(b, 256)
}

func BenchmarkPutByte4096(b *testing.B) {
	benchPutWithSize(b, 4096)
}

func benchPutWithSize(b *testing.B, size int) {
	db := setupTestDb()
	defer cleanup(db)

	key := make([]byte, size)
	value := make([]byte, size)
	for i := 0; i < b.N; i++ {
		rand.Read(key)
		rand.Read(value)
		db.Put(key, value)
	}

}
