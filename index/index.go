package index

import (
	"bitcask/wal"
)

type Indexer interface {
	Put(key []byte, pos *wal.ChunkPosition) *wal.ChunkPosition
	Get(key []byte) *wal.ChunkPosition
	Delete(key []byte) (*wal.ChunkPosition, bool)
	Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))
	AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))
	AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))
	Descend(handleFn func(key []byte, pos *wal.ChunkPosition) (bool, error))
	DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))
	DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error))
	Iterator(reverse bool) IndexIterator
	Size() int // 索引中数据个数
	Close() error
}

type IndexerType = int8

const (
	// BTREE 索引
	BTREE IndexerType = iota
)

var indexType = BTREE

func NewIndexer() Indexer {
	switch indexType {
	case BTREE:
		return newBTree()
	default:
		panic("unsupported index type")
	}
}

type IndexIterator interface {
	Rewind()                   // 回到起点
	Seek(key []byte)           // 找到第一个大于等于key，根据这个key开始遍历
	Next()                     // 下一个key
	Valid() bool               // 是否已经遍历完了所有的key，是否有效
	Key() []byte               // 当前遍历key数据
	Value() *wal.ChunkPosition // 遍历当前位置的value数据
	Close()                    // 关闭迭代器，施放相应资源
}
