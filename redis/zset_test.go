package redis

import (
	"bitcask"
	"bitcask/utils"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"testing"
)

func TestRedisDataStructure_ZScore(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-zset")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	ok, err := rds.ZAdd(utils.GetTestKey(1), 113, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.ZAdd(utils.GetTestKey(1), 333, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
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

func TestRedisDataStructure_ZRem(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-zrem")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	ok, err := rds.ZAdd([]byte("lcy"), 113, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.ZAdd([]byte("lcy"), 333, []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.ZAdd([]byte("lcy"), 98, []byte("val-2"))
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = rds.ZRem([]byte("lcy"), []byte("val-1"))
	assert.Nil(t, err)
	assert.True(t, ok)
	score, err := rds.ZScore([]byte("lcy"), []byte("val-1"))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
	assert.Equal(t, math.Inf(-1), score)

	ok, err = rds.ZRem([]byte("lcy"), []byte("val-1"))
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestRedisDataStructure_ZRange(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-zrange")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	test_data := map[string]float64{
		"aa":  3,
		"bb":  2,
		"cc":  7,
		"dd":  8,
		"ee":  1,
		"ff":  10,
		"aaa": 4,
		"aab": 3,
	}

	for k, v := range test_data {
		ok, err := rds.ZAdd([]byte("lcy"), v, []byte(k))
		assert.Nil(t, err)
		assert.True(t, ok)
	}

	n, err := rds.ZCard([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(len(test_data)), n)

	s, err := rds.ZRange([]byte("lcy"), 0, 2, false)
	assert.Nil(t, err)
	t.Log(s)

	s, err = rds.ZRange([]byte("lcy"), 0, -1, true)
	assert.Nil(t, err)
	t.Log(s)
}

func TestRedisDataStructure_ZRangeByScore(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-zrangebyscore")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	test_data := map[string]float64{
		"aa":  30,
		"bb":  25,
		"cc":  75,
		"dd":  80,
		"ee":  15,
		"ff":  10,
		"aaa": 45,
		"aab": 105,
	}

	for k, v := range test_data {
		ok, err := rds.ZAdd([]byte("lcy"), v, []byte(k))
		assert.Nil(t, err)
		assert.True(t, ok)
	}

	n, err := rds.ZCard([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(len(test_data)), n)

	s, err := rds.ZRangeByScore([]byte("lcy"), 15, 100, false, true, 0, 0)
	assert.Nil(t, err)
	t.Log(s)

	s, err = rds.ZRangeByScore([]byte("lcy"), 40, 90, true, true, 0, 0)
	assert.Nil(t, err)
	t.Log(s)

	s, err = rds.ZRangeByScore([]byte("lcy"), 40, 90, true, true, 0, 2)
	assert.Nil(t, err)
	t.Log(s)

	s, err = rds.ZRangeByScore([]byte("lcy"), 40, 190, true, true, 1, 2)
	assert.Nil(t, err)
	t.Log(s)
}

func TestRedisDataStructure_ZRank(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-redis-zrank")
	opts.DirPath = dir
	rds, err := NewRedisDataStructure(opts)
	defer closeDB(rds, dir)
	assert.Nil(t, err)

	test_data := map[string]float64{
		"aa":  30,
		"bb":  25,
		"cc":  75,
		"dd":  80,
		"ee":  15,
		"ff":  10,
		"aaa": 45,
		"aab": 105,
	}

	for k, v := range test_data {
		ok, err := rds.ZAdd([]byte("lcy"), v, []byte(k))
		assert.Nil(t, err)
		assert.True(t, ok)
	}

	n, err := rds.ZCard([]byte("lcy"))
	assert.Nil(t, err)
	assert.Equal(t, uint32(len(test_data)), n)

	s, err := rds.ZRange([]byte("lcy"), 0, -1, true)
	assert.Nil(t, err)
	t.Log(s)

	rank, err := rds.ZRank([]byte("lcy"), []byte("ee"), false)
	assert.Nil(t, err)
	t.Log(rank)
	assert.Equal(t, 1, rank)

	rank, err = rds.ZRank([]byte("lcy"), []byte("aab"), false)
	assert.Nil(t, err)
	t.Log(rank)
	assert.Equal(t, 7, rank)

	rank, err = rds.ZRank([]byte("lcy"), []byte("aab"), true)
	assert.Nil(t, err)
	t.Log(rank)
	assert.Equal(t, 0, rank)
}
