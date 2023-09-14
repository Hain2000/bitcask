package index

import (
	"bitcask/data"
	"github.com/google/btree"
	"sync"
)

type Btree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *Btree {
	return &Btree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *Btree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *Btree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *Btree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oidItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oidItem == nil {
		return false
	}
	return true
}
