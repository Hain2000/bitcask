package index

import (
	"bitcask/wal"
	"bytes"
	"github.com/google/btree"
	"sync"
)

type MemoryBtree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

type item struct {
	key []byte
	pos *wal.ChunkPosition
}

func (x *item) Less(bi btree.Item) bool {
	if bi == nil {
		return false
	}
	return bytes.Compare(x.key, bi.(*item).key) < 0
}

func newBTree() *MemoryBtree {
	return &MemoryBtree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *MemoryBtree) Put(key []byte, pos *wal.ChunkPosition) *wal.ChunkPosition {
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldValue := bt.tree.ReplaceOrInsert(&item{key: key, pos: pos})
	if oldValue != nil {
		return oldValue.(*item).pos
	}
	return nil
}

func (bt *MemoryBtree) Get(key []byte) *wal.ChunkPosition {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	val := bt.tree.Get(&item{key: key})
	if val != nil {
		return val.(*item).pos
	}
	return nil
}

func (bt *MemoryBtree) Delete(key []byte) (*wal.ChunkPosition, bool) {
	bt.lock.Lock()
	defer bt.lock.Unlock()
	val := bt.tree.Delete(&item{key: key})
	if val != nil {
		return val.(*item).pos, true
	}
	return nil, false
}

func (bt *MemoryBtree) Size() int {
	return bt.tree.Len()
}

func (bt *MemoryBtree) Close() error {
	return nil
}

func (bt *MemoryBtree) Ascend(handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	bt.tree.Ascend(func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (bt *MemoryBtree) AscendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	bt.tree.AscendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (bt *MemoryBtree) AscendGreaterOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	bt.tree.AscendGreaterOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (bt *MemoryBtree) Descend(handleFn func(key []byte, pos *wal.ChunkPosition) (bool, error)) {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	bt.tree.Descend(func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (bt *MemoryBtree) DescendRange(startKey, endKey []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	bt.tree.DescendRange(&item{key: startKey}, &item{key: endKey}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (bt *MemoryBtree) DescendLessOrEqual(key []byte, handleFn func(key []byte, position *wal.ChunkPosition) (bool, error)) {
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	bt.tree.DescendLessOrEqual(&item{key: key}, func(i btree.Item) bool {
		cont, err := handleFn(i.(*item).key, i.(*item).pos)
		if err != nil {
			return false
		}
		return cont
	})
}

func (bt *MemoryBtree) Iterator(reverse bool) IndexIterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBtreeIterator(bt.tree, reverse)
}

// BTree 索引迭代器
type btreeIterator struct {
	tree    *btree.BTree
	current *item // 当前正在遍历的元素
	reverse bool  // 是否反向
	valid   bool
}

func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var current *item
	var valid bool
	if tree.Len() > 0 {
		if reverse {
			current = tree.Max().(*item)
		} else {
			current = tree.Min().(*item)
		}
		valid = true
	}
	return &btreeIterator{
		tree:    tree.Clone(),
		current: current,
		reverse: reverse,
		valid:   valid,
	}
}

func (it *btreeIterator) Rewind() {
	if it.tree == nil || it.tree.Len() == 0 {
		return
	}
	if it.reverse {
		it.current = it.tree.Max().(*item)
	} else {
		it.current = it.tree.Min().(*item)
	}
	it.valid = true
}

func (it *btreeIterator) Seek(key []byte) {
	if it.tree == nil || !it.valid {
		return
	}
	seekItem := &item{key: key}
	it.valid = false
	if it.reverse {
		it.tree.DescendLessOrEqual(seekItem, func(i btree.Item) bool {
			it.current = i.(*item)
			it.valid = true
			return false
		})
	} else {
		it.tree.AscendGreaterOrEqual(seekItem, func(i btree.Item) bool {
			it.current = i.(*item)
			it.valid = true
			return false
		})
	}
}

func (it *btreeIterator) Next() {
	if it.tree == nil || !it.valid {
		return
	}
	it.valid = false
	if it.reverse {
		it.tree.DescendLessOrEqual(it.current, func(i btree.Item) bool {
			if !i.(*item).Less(it.current) {
				return true
			}
			it.current = i.(*item)
			it.valid = true
			return false
		})
	} else {
		it.tree.AscendGreaterOrEqual(it.current, func(i btree.Item) bool {
			if !it.current.Less(i.(*item)) {
				return true
			}
			it.current = i.(*item)
			it.valid = true
			return false
		})
	}
	if !it.valid {
		it.current = nil
	}
}

func (it *btreeIterator) Valid() bool {
	return it.valid
}

func (it *btreeIterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.current.key
}

func (it *btreeIterator) Value() *wal.ChunkPosition {
	if !it.valid {
		return nil
	}
	return it.current.pos
}

func (it *btreeIterator) Close() {
	it.tree.Clear(true)
	it.tree = nil
	it.current = nil
	it.valid = false
}
