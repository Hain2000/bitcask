package bitcask

import (
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
)

func destroyDB(db *DB) {
	_ = db.Close()
	_ = os.RemoveAll(db.options.DirPath)
}

func TestDB_Put_Normal(t *testing.T) {
	options := DefaultOptions
	db, err := Open(options)
	assert.Nil(t, err)
	defer destroyDB(db)

	for i := 0; i < 100; i++ {
		err := db.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(128))
		assert.Nil(t, err)
		err = db.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(KB))
		assert.Nil(t, err)
		err = db.Put(utils.GetTestKey(rand.Int()), utils.RandomValue(5*KB))
		assert.Nil(t, err)
	}

	// reopen
	err = db.Close()
	assert.Nil(t, err)
	db2, err := Open(options)
	assert.Nil(t, err)
	defer func() {
		_ = db2.Close()
	}()
	stat := db2.Stat()
	assert.Equal(t, 300, stat.KeyNum)
}
