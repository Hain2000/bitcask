package benchmark

import (
	"bitcask"
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
	"os"
	"testing"
)

var db *bitcask.DB

func openDB() func() {
	options := bitcask.DefaultOptions
	options.DirPath = "/tmp/bitcask_bench_test"
	var err error
	db, err = bitcask.Open(options)
	if err != nil {
		panic(err)
	}

	return func() {
		_ = db.Close()
		_ = os.RemoveAll(options.DirPath)
	}
}

func BenchmarkPutGetDelete(b *testing.B) {
	closer := openDB()
	defer closer()

	b.Run("put", benchmarkPut)
	b.Run("get", bencharkGet)
	b.Run("delete", bencharkDelete)
}

func benchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func bencharkGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = db.Get(utils.GetTestKey(rand.Int()))
	}
}

func bencharkDelete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(b, err)
	}
}
