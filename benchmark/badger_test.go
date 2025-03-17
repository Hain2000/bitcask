package benchmark

import (
	"github.com/Hain2000/bitcask/utils"
	"github.com/dgraph-io/badger/v4"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var bdb *badger.DB

func openDB4() func() {
	var err error
	opts := badger.DefaultOptions("/tmp/badger_bench_test").WithLogger(nil) // 关闭日志减少干扰
	bdb, err = badger.Open(opts)
	if err != nil {
		panic(err)
	}

	return func() {
		_ = bdb.Close()
		_ = os.RemoveAll("/tmp/badger_bench_test")
	}
}

func BenchmarkPutGetDelete4(b *testing.B) {
	closer := openDB4()
	defer closer()

	b.Run("put", benchmarkPut4)
	b.Run("get", benchmarkGet4)
	b.Run("delete", benchmarkDelete4)
}

func benchmarkPut4(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := bdb.Update(func(txn *badger.Txn) error {
			return txn.Set(utils.GetTestKey(i), utils.RandomValue(1024))
		})
		assert.Nil(b, err)
	}
}

func benchmarkGet4(b *testing.B) {
	// 预写入数据
	for i := 0; i < b.N; i++ {
		err := bdb.Update(func(txn *badger.Txn) error {
			return txn.Set(utils.GetTestKey(i), utils.RandomValue(1024))
		})
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = bdb.View(func(txn *badger.Txn) error {
			_, err := txn.Get(utils.GetTestKey(rand.Int()))
			return err
		})
	}
}

func benchmarkDelete4(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := bdb.Update(func(txn *badger.Txn) error {
			return txn.Delete(utils.GetTestKey(i))
		})
		assert.Nil(b, err)
	}
}
