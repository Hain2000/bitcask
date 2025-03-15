package redis

import (
	"bitcask"
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func closeDB(rds *DataStructure, dir string) {
	_ = rds.Close()
	_ = os.RemoveAll(dir)
}

func TestString_SetGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-setget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(1), utils.RandomValue(100), 0)
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), utils.RandomValue(100), time.Second*2)
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	time.Sleep(time.Second * 3)
	_, err = rds.Get(utils.GetTestKey(2))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)

	_, err = rds.Get(utils.GetTestKey(33))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestString_MSetGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-msetget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	data := []string{"k1", "v1", "k2", "v2", "k3", "v3"}

	n, err := rds.MSet(data)
	assert.Nil(t, err)
	assert.Equal(t, 3, n)

	val, err := rds.Get([]byte("k2"))
	assert.Nil(t, err)
	assert.Equal(t, "v2", string(val))

	val, err = rds.Get([]byte("k4"))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)

	keys := []string{"k1", "k2", "k3", "k4"}
	vals, err := rds.MGet(keys)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(vals))
	t.Log(vals)
}

func TestString_SetNx(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-msetget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	err = rds.Set([]byte("lcy"), []byte("qwq"), 0)
	assert.Nil(t, err)
	_, err = rds.SetNX([]byte("lcy"), []byte("yyds"))
	assert.Nil(t, err)

	val, err := rds.Get([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, "qwq", string(val))

	_, err = rds.SetNX([]byte("chunyu"), []byte("yyds"))
	assert.Nil(t, err)
	_, err = rds.SetNX([]byte("chunyu"), []byte("114514"))
	assert.Nil(t, err)
	val, err = rds.Get([]byte("chunyu"))
	assert.Nil(t, err)
	assert.Equal(t, "yyds", string(val))
}
