package benchmark

import (
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
	"golang.org/x/exp/rand"
	"os"
	"testing"
)

var ldb *leveldb.DB

func openDB2() func() {
	var err error
	ldb, err = leveldb.OpenFile("/tmp/leveldb_bench_test", nil)
	if err != nil {
		panic(err)
	}

	return func() {
		_ = ldb.Close()
		_ = os.RemoveAll("/tmp/leveldb_bench_test")
	}
}

func BenchmarkPutGetDelete2(b *testing.B) {
	closer := openDB2()
	defer closer()

	b.Run("put", benchmarkPut2)
	b.Run("get", bencharkGet2)
	b.Run("delete", bencharkDelete2)
}

func benchmarkPut2(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := ldb.Put(utils.GetTestKey(i), utils.RandomValue(1024), nil)
		assert.Nil(b, err)
	}
}

func bencharkGet2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := ldb.Put(utils.GetTestKey(i), utils.RandomValue(1024), nil)
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = ldb.Get(utils.GetTestKey(rand.Int()), nil)
	}
}

func bencharkDelete2(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := ldb.Delete(utils.GetTestKey(i), nil)
		assert.Nil(b, err)
	}
}
