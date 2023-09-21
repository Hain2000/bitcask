package index

import (
	"bitcask/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// 封装 github.com/plar/go-adaptive-radix-tree

// AdaptiveRadixTree 自适应索引树
type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldVal, _ := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if oldVal == nil {
		return nil
	}
	return oldVal.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	defer art.lock.Unlock()
	oldVal, deleted := art.tree.Delete(key)
	if oldVal == nil {
		return nil, false
	}
	return oldVal.(*data.LogRecordPos), deleted
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	if art == nil {
		return nil
	}
	return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	size := art.tree.Size()
	return size
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

// Art 索引迭代器
type artIterator struct {
	curIndex int
	reverse  bool    // 是否反向
	values   []*Item // [key] = 位置索引信息
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	values := make([]*Item, tree.Size())
	if reverse {
		idx = tree.Size() - 1
	}
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues)

	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (ai *artIterator) Rewind() {
	ai.curIndex = 0
}

func (ai *artIterator) Seek(key []byte) {
	if ai.reverse {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0 // 找到一个i使得 [i].key >= key
		})
	} else {
		ai.curIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}

}

func (ai *artIterator) Next() {
	ai.curIndex++
}

func (ai *artIterator) Valid() bool {
	return ai.curIndex < len(ai.values)
}

func (ai *artIterator) Key() []byte {
	return ai.values[ai.curIndex].key
}

func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.curIndex].pos
}

func (ai *artIterator) Close() {
	ai.values = nil
}
