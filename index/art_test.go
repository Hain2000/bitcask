package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 23})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 23})
	art.Put([]byte("key-3"), &data.LogRecordPos{Fid: 1, Offset: 23})
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 23})
	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)
	pos1 := art.Get([]byte("YYDS"))
	assert.Nil(t, pos1)
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 122, Offset: 233})
	pos2 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos2)
	t.Log(pos2)
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()

	res0 := art.Delete([]byte("YYDS"))
	t.Log(res0)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 23})

	pos := art.Get([]byte("key-1"))
	assert.NotNil(t, pos)
	res1 := art.Delete([]byte("key-1"))
	assert.True(t, res1)
	pos1 := art.Get([]byte("key-1"))
	assert.Nil(t, pos1)
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 122, Offset: 233})
	pos2 := art.Get([]byte("key-1"))
	assert.NotNil(t, pos2)

}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()
	assert.Equal(t, 0, art.Size())
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 23})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 23})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 23})
	assert.Equal(t, 2, art.Size())

}

func TestAdaptiveRadixTree_Iterator(t *testing.T) {
	art := NewART()
	art.Put([]byte("bbb"), &data.LogRecordPos{Fid: 1, Offset: 23})
	art.Put([]byte("bdd"), &data.LogRecordPos{Fid: 1, Offset: 23})
	art.Put([]byte("ccc"), &data.LogRecordPos{Fid: 1, Offset: 23})
	art.Put([]byte("aaa"), &data.LogRecordPos{Fid: 1, Offset: 23})
	art.Put([]byte("aba"), &data.LogRecordPos{Fid: 1, Offset: 23})
	art.Put([]byte("abc"), &data.LogRecordPos{Fid: 1, Offset: 23})
	iter := art.Iterator(true)

	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
