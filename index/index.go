package index

import (
	"bitcask/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool //
	Get(key []byte) *data.LogRecordPos           //
	Delete(key []byte) bool                      //
	Iterator(reverse bool) Iterator
	Size() int // 索引中数据个数
	Close() error
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (x *Item) Less(y btree.Item) bool {
	return bytes.Compare(x.key, y.(*Item).key) == -1
}

type IdxType = int8

const (
	// BTREE 索引
	BTREE IdxType = iota + 1

	// ART 自适应基数索引
	ART

	// BPLUSTREE B+树索引
	BPLUSTREE
)

func NewIndexer(ty IdxType, dirPath string, sync bool) Indexer {
	switch ty {
	case BTREE:
		return NewBTree()
	case ART:
		return NewART()
	case BPLUSTREE:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

type Iterator interface {
	Rewind()                   // 回到起点
	Seek(key []byte)           // 找到第一个大于等于key，根据这个key开始遍历
	Next()                     // 下一个key
	Valid() bool               // 是否已经遍历完了所有的key，是否有效
	Key() []byte               // 当前遍历key数据
	Value() *data.LogRecordPos // 遍历当前位置的value数据
	Close()                    // 关闭迭代器，施放相应资源
}
