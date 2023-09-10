package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBtree_Put(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)
	res2 := bt.Put([]byte("yyds"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.True(t, res2)
}

func TestBtree_Get(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)

	getres := bt.Get(nil)
	assert.Equal(t, uint32(1), getres.Fid)
	assert.Equal(t, int64(100), getres.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 2,
	})
	assert.True(t, res2)

	res3 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 3,
	})
	assert.True(t, res3)

	getres2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), getres2.Fid)
	assert.Equal(t, int64(3), getres2.Offset)
}

func TestBtree_Delete(t *testing.T) {
	bt := NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.True(t, res)
	res2 := bt.Delete(nil)
	assert.True(t, res2)

	res3 := bt.Put([]byte("yyds"), &data.LogRecordPos{Fid: 1, Offset: 22})
	assert.True(t, res3)
	res4 := bt.Delete([]byte("yyds"))
	assert.True(t, res4)

}
