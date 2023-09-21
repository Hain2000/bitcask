package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res)
	res2 := bt.Put([]byte("yyds"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("yyds"), &data.LogRecordPos{
		Fid:    21,
		Offset: 12,
	})
	assert.Equal(t, uint32(1), res3.Fid)
	assert.Equal(t, int64(2), res3.Offset)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res)

	getres := bt.Get(nil)
	assert.Equal(t, uint32(1), getres.Fid)
	assert.Equal(t, int64(100), getres.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.Nil(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 3,
	})
	assert.NotNil(t, res3)
	assert.Equal(t, uint32(1), res3.Fid)
	assert.Equal(t, int64(2), res3.Offset)
	getres2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), getres2.Fid)
	assert.Equal(t, int64(3), getres2.Offset)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res)
	res2, ok1 := bt.Delete(nil)
	assert.NotNil(t, res2)
	assert.True(t, ok1)

	res3 := bt.Put([]byte("yyds"), &data.LogRecordPos{Fid: 1, Offset: 22})
	assert.Nil(t, res3)
	res4, ok2 := bt.Delete([]byte("yyds"))
	assert.NotNil(t, res4)
	assert.True(t, ok2)
	assert.Equal(t, uint32(1), res4.Fid)
	assert.Equal(t, int64(22), res4.Offset)
}

func TestBtree_Iterator(t *testing.T) {
	bt1 := NewBTree()

	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	bt1.Put([]byte("YYDS"), &data.LogRecordPos{Fid: 1, Offset: 10})

	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())

	bt1.Put([]byte("CCC"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("BBB"), &data.LogRecordPos{Fid: 1, Offset: 10})
	bt1.Put([]byte("AAA"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	iter5 := bt1.Iterator(false)
	iter5.Seek([]byte("ABB"))
	t.Log(string(iter5.Key()))
	for iter5.Seek([]byte("ABB")); iter5.Valid(); iter5.Next() {
		t.Log(string(iter5.Key()))
	}

	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("CBA")); iter6.Valid(); iter6.Next() {
		t.Log(string(iter6.Key()))
	}
}
