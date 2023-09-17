package bitcask

import (
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-iterator-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	defer closeFiles(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestIterator_value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-iterator-2")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	defer closeFiles(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())

}

func TestIterator_multi_values(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-iterator-3")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	defer closeFiles(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put([]byte("aaaaa"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aabaa"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("aabac"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("daaca"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("babaa"), utils.RandomValue(10))
	assert.Nil(t, err)
	err = db.Put([]byte("baaaa"), utils.RandomValue(10))
	assert.Nil(t, err)
	iter1 := db.NewIterator(DefaultIteratorOptions)
	iter1.ForEach(func() {
		assert.NotNil(t, iter1.Key())
	})
	iter1.Rewind()
	for iter1.Seek([]byte("bc")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}

	iterOpts1 := DefaultIteratorOptions
	iterOpts1.Reverse = true
	iter2 := db.NewIterator(iterOpts1)
	iter2.ForEach(func() {
		// t.Log("key =", string(iter2.Key()))
	})
	iter2.Rewind()
	for iter2.Seek([]byte("bc")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
		// t.Log(string(iter2.Key()))
	}

	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("b")
	iter3 := db.NewIterator(iterOpts2)
	iter3.ForEach(func() {
		assert.NotNil(t, iter3.Key())
	})
}

func TestDB_CurListKeys(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-list-key")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	defer closeFiles(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.CurListKeys()
	assert.Equal(t, 0, len(keys))

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(23))
	assert.Nil(t, err)
	keys2 := db.CurListKeys()
	assert.Equal(t, 1, len(keys2))

	err = db.Put(utils.GetTestKey(21), utils.RandomValue(23))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(31), utils.RandomValue(23))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(41), utils.RandomValue(23))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(51), utils.RandomValue(23))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(61), utils.RandomValue(23))
	assert.Nil(t, err)
	keys2 = db.CurListKeys()
	assert.Equal(t, 6, len(keys2))
	for _, k := range keys2 {
		assert.NotNil(t, k)
	}

}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-fold")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	defer closeFiles(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(21), utils.RandomValue(23))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(31), utils.RandomValue(23))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(41), utils.RandomValue(23))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(51), utils.RandomValue(23))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(61), utils.RandomValue(23))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}
