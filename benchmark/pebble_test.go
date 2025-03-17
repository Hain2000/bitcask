package benchmark

import (
	"github.com/Hain2000/bitcask/utils"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
	"os"
	"testing"
)

var pdb *pebble.DB

func openDB3() func() {
	var err error
	pdb, err = pebble.Open("/tmp/pebble_bench_test", &pebble.Options{})
	if err != nil {
		panic(err)
	}

	return func() {
		_ = pdb.Close()
		_ = os.RemoveAll("/tmp/pebble_bench_test")
	}
}

func BenchmarkPutGetDelete3(b *testing.B) {
	closer := openDB3()
	defer closer()

	b.Run("put", benchmarkPut3)
	b.Run("get", bencharkGet3)
	b.Run("delete", bencharkDelete3)
}

func benchmarkPut3(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := pdb.Set(utils.GetTestKey(i), utils.RandomValue(1024), pebble.NoSync)
		assert.Nil(b, err)
	}
}

func bencharkGet3(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := pdb.Set(utils.GetTestKey(i), utils.RandomValue(1024), pebble.NoSync)
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _ = pdb.Get(utils.GetTestKey(rand.Int()))
	}
}

func bencharkDelete3(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := pdb.Delete(utils.GetTestKey(i), pebble.NoSync)
		assert.Nil(b, err)
	}
}
