package redis

import (
	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
)

func TestRedisDataStructure_SIsMember(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-sismember")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
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
	defer closeDB(rds, dir)
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

func TestRedisDataStructure_SMembers(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-smembers")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	for i := 0; i < 10; i++ {
		ok, err := rds.SAdd([]byte("lcy"), []byte(strconv.FormatUint(uint64(i), 10)))
		assert.Nil(t, err)
		assert.True(t, ok)
	}

	res1, err := rds.SMembers([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, 10, len(res1))
	t.Log(res1)

	n, err := rds.SCard([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(10), n)

	ok, err := rds.SRem([]byte("lcy"), []byte("8"))
	assert.Nil(t, err)
	assert.True(t, ok)

	res2, err := rds.SMembers([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, 9, len(res2))
	t.Log(res2)

	n, err = rds.SCard([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(9), n)

	ok, err = rds.SRem([]byte("lcy"), []byte("114514"))
	assert.Nil(t, err)
	assert.False(t, ok)

	res2, err = rds.SMembers([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, 9, len(res2))
	t.Log(res2)

	n, err = rds.SCard([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(9), n)
}
