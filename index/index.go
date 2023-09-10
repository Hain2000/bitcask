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
