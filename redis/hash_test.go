package redis

import (
	"bitcask"
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDataStructure_HashInternalKey(t *testing.T) {
	hk := hashInternalKey{
		key:   []byte("lcy"),
		filed: []byte("yyds"),
	}
	encKey := hk.encode()
	filed := decodeFiled(encKey, []byte("lcy"))
	assert.NotNil(t, filed)
	assert.Equal(t, hk.filed, filed)

	hk = hashInternalKey{
		key:   []byte("lcyy"),
		filed: []byte("yds"),
	}
	encKey = hk.encode()
	filed = decodeFiled(encKey, []byte("lcy"))
	assert.Nil(t, filed)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-hget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer close(rds, dir)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v1 := utils.RandomValue(110)
	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), v1)
	assert.Nil(t, err)
	assert.True(t, ok2)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, v1, val1)

	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.Equal(t, v2, val2)

	_, err = rds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-hdel")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer close(rds, dir)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.GetTestKey(200), nil)
	assert.Nil(t, err)
	assert.False(t, del1)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	assert.Nil(t, err)
	assert.True(t, ok1)

	v2 := utils.RandomValue(100)
	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), v2)
	assert.Nil(t, err)
	assert.True(t, ok3)

	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.True(t, del2)

	_, err = rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HMSetGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-hmsetget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer close(rds, dir)
	assert.Nil(t, err)
	mp1 := map[string]string{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	}
	n1, err := rds.HMSet([]byte("lcy"), mp1)
	assert.Nil(t, err)
	assert.Equal(t, len(mp1), n1)

	v1, err := rds.HGet([]byte("lcy"), []byte("field1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), v1)

	mp2 := map[string]string{
		"field1": "value1",
		"field2": "value2",
		"field4": "value4",
	}
	n2, err := rds.HMSet([]byte("lcy"), mp2)
	assert.Nil(t, err)
	assert.Equal(t, 1, n2)

	mp := map[string]string{
		"field1": "value1",
		"field3": "value3",
		"field4": "value4",
	}
	fields := []string{"field1", "field3", "field4"}
	v2, err := rds.HMGet([]byte("lcy"), fields...)
	assert.Nil(t, err)
	assert.Equal(t, mp, v2)
}

func TestRedisDataStructure_HExist(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-hmsetget")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer close(rds, dir)
	assert.Nil(t, err)

	exist1, err := rds.HExist([]byte("lcy"), []byte("f1"))
	assert.Nil(t, err)
	assert.False(t, exist1)

	ok, err := rds.HSet([]byte("lcy"), []byte("f1"), []byte("v1"))
	assert.Nil(t, err)
	assert.True(t, ok)

	exist2, err := rds.HExist([]byte("lcy"), []byte("f1"))
	assert.Nil(t, err)
	assert.True(t, exist2)
}

func TestRedisDataStructure_HKeysVals(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-kvs")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer close(rds, dir)
	assert.Nil(t, err)

	mp1 := map[string]string{
		"yyds1": "value1",
		"yyds2": "value2",
		"yyds3": "value3",
	}
	n1, err := rds.HMSet([]byte("lcy"), mp1)
	assert.Nil(t, err)
	assert.Equal(t, len(mp1), n1)

	v, err := rds.HGet([]byte("lcy"), []byte("yyds1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value1"), v)

	keys, err := rds.HKeys([]byte("lcy"))
	assert.Nil(t, err)
	t.Log(keys)
	vals, err := rds.HVals([]byte("lcy"))
	assert.Nil(t, err)
	t.Log(vals)

	kvs, err := rds.HGetAll([]byte("lcy"))
	assert.Nil(t, err)
	t.Log(kvs)

	keys, err = rds.HKeys([]byte("lcyy"))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(keys))

	keys, err = rds.HKeys([]byte("lc"))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(keys))
}

func TestRedisDataStructure_HIncrBy(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-hincr")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer close(rds, dir)
	assert.Nil(t, err)
	x, err := rds.HIncrBy([]byte("lcy"), []byte("f1"), 1)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), x)
	x, err = rds.HIncrBy([]byte("lcy"), []byte("f1"), 114514)
	assert.Nil(t, err)
	assert.Equal(t, int64(114515), x)
}

func TestRedisDataStructure_HSetNX(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-hsetnx")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer close(rds, dir)
	assert.Nil(t, err)

	mp1 := map[string]string{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	}
	n1, err := rds.HMSet([]byte("lcy"), mp1)
	assert.Nil(t, err)
	assert.Equal(t, len(mp1), n1)

	ok, err := rds.HSetNX([]byte("lcy"), []byte("field1"), []byte("value2"))
	assert.Nil(t, err)
	assert.False(t, ok)

	ok2, err := rds.HSetNX([]byte("lcy"), []byte("f1"), []byte("value2"))
	assert.Nil(t, err)
	assert.True(t, ok2)

	val, err := rds.HGet([]byte("lcy"), []byte("f1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("value2"), val)
}
