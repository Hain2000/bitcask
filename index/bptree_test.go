package index

import (
	"bitcask/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		os.RemoveAll(path) // os.Remove() 只能删除空目录，而 os.RemoveAll() 不受任何限制，都可以删除。
	}()
	tree := NewBPlusTree(path, false)

	res1 := tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 123, Offset: 212})
	assert.Nil(t, res1)
	res2 := tree.Put([]byte("abb"), &data.LogRecordPos{Fid: 123, Offset: 212})
	assert.Nil(t, res2)
	res3 := tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 212})
	assert.Nil(t, res3)

	res4 := tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 555, Offset: 132})
	assert.Equal(t, uint32(123), res4.Fid)
	assert.Equal(t, int64(212), res4.Offset)

}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		os.RemoveAll(path)
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
		os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	res, ok := tree.Delete([]byte("yyds"))
	assert.Equal(t, false, ok)
	assert.Nil(t, res)
	tree.Put([]byte("aaa"), &data.LogRecordPos{Fid: 123, Offset: 212})
	res1, ok2 := tree.Delete([]byte("aaa"))
	assert.Equal(t, uint32(123), res1.Fid)
	assert.Equal(t, int64(212), res1.Offset)
	assert.True(t, ok2)
	pos1 := tree.Get([]byte("aaa"))
	assert.Nil(t, pos1)

}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree")
	os.MkdirAll(path, os.ModePerm)
	defer func() {
		os.RemoveAll(path)
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
		os.RemoveAll(path)
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
