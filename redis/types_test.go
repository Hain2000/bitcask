package redis

import (
	"bitcask"
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-get")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	rds.Del(utils.GetTestKey(11))

	err = rds.Set(utils.GetTestKey(1), utils.RandomValue(100), 0)
	assert.Nil(t, err)

	tp, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, tp)

	rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)

}

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-sismember")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-not-exist"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_SRem(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-srem")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SAdd(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SRem(utils.GetTestKey(2), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.SRem(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_ZScore(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-zset")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.ZAdd(utils.GetTestKey(1), 113, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 333, []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 98, []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	score, err := rds.ZScore(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, float64(333), score)
	score, err = rds.ZScore(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, float64(98), score)
}
