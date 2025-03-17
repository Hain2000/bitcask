package redis

import (
	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestRedisDataStructure_LPop(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-lpop")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	res, err := rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.LPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestRedisDataStructure_RPop(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-rpop")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	res, err := rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(1), res)
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-1"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(2), res)
	res, err = rds.RPush(utils.GetTestKey(1), []byte("val-2"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(3), res)

	val, err := rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
	val, err = rds.RPop(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val)
}

func TestRedisDataStructure_LRange(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-lrange")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	for i := uint32(0); i < 5; i++ {
		x, err := rds.LPush([]byte("lcy"), []byte(strconv.FormatUint(uint64(i), 10)))
		assert.Nil(t, err)
		assert.Equal(t, i+1, x)
	}
	for i := uint32(5); i < 10; i++ {
		x, err := rds.RPush([]byte("lcy"), []byte(strconv.FormatUint(uint64(i), 10)))
		assert.Nil(t, err)
		assert.Equal(t, i+1, x)
	}
	n, err := rds.LLen([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, 10, n)
	s, err := rds.LRange([]byte("lcy"), 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 10, len(s))
	t.Log(s)

	s, err = rds.LRange([]byte("lcy"), 5, -1)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(s))
	t.Log(s)

	s, err = rds.LRange([]byte("lcy"), 100, -1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(s))
	t.Log(s)

	s, err = rds.LRange([]byte("lcy"), -8, -4)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(s))
	t.Log(s)
}

func TestRedisDataStructure_LTrim(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-ltrim")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	for i := uint32(0); i < 5; i++ {
		x, err := rds.LPush([]byte("lcy"), []byte(strconv.FormatUint(uint64(i), 10)))
		assert.Nil(t, err)
		assert.Equal(t, i+1, x)
	}
	for i := uint32(5); i < 10; i++ {
		x, err := rds.RPush([]byte("lcy"), []byte(strconv.FormatUint(uint64(i), 10)))
		assert.Nil(t, err)
		assert.Equal(t, i+1, x)
	}
	err = rds.LTrim([]byte("lcy"), 2, 8)
	assert.Nil(t, err)

	s, err := rds.LRange([]byte("lcy"), 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 7, len(s))
	t.Log(s)

	err = rds.LTrim([]byte("lcy"), 2, 8)
	assert.Nil(t, err)
	s, err = rds.LRange([]byte("lcy"), 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(s))
	t.Log(s)

	err = rds.LTrim([]byte("lcy"), -1, 0)
	assert.Nil(t, err)
	s, err = rds.LRange([]byte("lcy"), 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(s))
	t.Log(s)
}

func TestRedisDataStructure_LInsert(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-linsert")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	for i := uint32(0); i < 5; i++ {
		x, err := rds.LPush([]byte("lcy"), []byte(strconv.FormatUint(uint64(i), 10)))
		assert.Nil(t, err)
		assert.Equal(t, i+1, x)
	}
	for i := uint32(5); i < 10; i++ {
		x, err := rds.RPush([]byte("lcy"), []byte(strconv.FormatUint(uint64(i), 10)))
		assert.Nil(t, err)
		assert.Equal(t, i+1, x)
	}
	ok, err := rds.LInsert([]byte("lcy"), []byte("0"), []byte("A"), true)
	assert.Nil(t, err)
	assert.True(t, ok)

	s, err := rds.LRange([]byte("lcy"), 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 11, len(s))
	t.Log(s)

	ok, err = rds.LInsert([]byte("lcy"), []byte("0"), []byte("B"), false)
	assert.Nil(t, err)
	assert.True(t, ok)

	s, err = rds.LRange([]byte("lcy"), 0, -1)
	assert.Nil(t, err)
	assert.Equal(t, 12, len(s))
	t.Log(s)
}

func TestRedisDataStructure_BPop(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-bpop")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)
	go func() {
		time.Sleep(1 * time.Second)
		_, err := rds.RPush([]byte("lcy"), []byte("114514"))
		assert.Nil(t, err)
	}()
	val, err := rds.BRPop([]byte("lcy"), 3*time.Second)
	assert.Nil(t, err)
	assert.Equal(t, []byte("114514"), val)

	go func() {
		time.Sleep(1 * time.Second)
		_, err := rds.LPush([]byte("lcy"), []byte("114515"))
		assert.Nil(t, err)
	}()
	val, err = rds.BRPop([]byte("lcy"), 3*time.Second)
	assert.Nil(t, err)
	assert.Equal(t, []byte("114515"), val)

	go func() {
		time.Sleep(2 * time.Second)
		_, err := rds.LPush([]byte("lcy"), []byte("114515"))
		assert.Nil(t, err)
	}()
	val, err = rds.BRPop([]byte("lcy"), 1*time.Second)
	assert.Nil(t, err)
	assert.Nil(t, val)
}
