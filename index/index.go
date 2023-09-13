package index

import (
	"bitcask/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool // 放存储位置
	Get(key []byte) *data.LogRecordPos           // 拿到位置
	Delete(key []byte) bool                      //
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
)

func NewIndexer(ty IdxType) Indexer {
	switch ty {
	case BTREE:
		return NewBTree()
	case ART:
		// TODO
		return nil
	default:
		panic("unsupported index type")
	}
}
