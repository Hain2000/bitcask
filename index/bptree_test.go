package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("aba"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 212})
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("yyds"))
	assert.Nil(t, pos)
	tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 123, Offset: 212})
	pos1 := tree.Get([]byte("aaa"))
	assert.NotNil(t, pos1)

	tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 123, Offset: 2122})
	pos2 := tree.Get([]byte("aaa"))
	assert.NotNil(t, pos2)

}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	res := tree.Delete([]byte("yyds"))
	assert.Equal(t, false, res)
	tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 123, Offset: 212})
	res1 := tree.Delete([]byte("aaa"))
	t.Log(res1)

	pos1 := tree.Get([]byte("aaa"))
	t.Log(pos1)

}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	t.Log(tree.Size())

	tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 21, Offset: 212})
	tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 21, Offset: 212})
	tree.Put([]byte("aab"), &data.LogRecordPos{Fid: 21, Offset: 212})
	tree.Put([]byte("aas"), &data.LogRecordPos{Fid: 21, Offset: 212})
	t.Log(tree.Size())
	tree.Delete([]byte("aaa"))
	t.Log(tree.Size())

}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		os.Remove(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("aba"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("qwceaa"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("terq"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("trer"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("qwtr"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("acx"), &data.LogRecordPos{Fid: 123, Offset: 212})
	tree.Put([]byte("yuc"), &data.LogRecordPos{Fid: 123, Offset: 212})

	iter := tree.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}

}
