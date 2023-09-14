package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/tmp", "yyds.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	defer fio.Close()
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Writer(t *testing.T) {
	path := filepath.Join("/tmp", "yyds.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
	n, err1 := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err1)
	n2, err2 := fio.Write([]byte("yyds"))
	assert.Equal(t, 4, n2)
	t.Log(n2, err2)
	n3, err3 := fio.Write([]byte("hello world"))
	t.Log(n3, err3)
	assert.Equal(t, 11, n3)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/tmp", "yyds.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err1 := fio.Write([]byte("key-x"))
	assert.Nil(t, err1)

	_, err2 := fio.Write([]byte("key-y"))
	assert.Nil(t, err2)

	b := make([]byte, 5)
	n, _ := fio.Read(b, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-x"), b)
	b2 := make([]byte, 5)
	n2, _ := fio.Read(b2, 5)
	assert.Equal(t, 5, n2)
	assert.Equal(t, []byte("key-y"), b2)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/tmp", "yyds.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err2 := fio.Sync()
	assert.Nil(t, err2)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/tmp", "yyds.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err2 := fio.Close()
	assert.Nil(t, err2)
}

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}
