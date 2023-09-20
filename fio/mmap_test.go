package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "mmap-read.data")
	defer destroyFile(path)

	mmapIO, err := NewMMapIOManager(path)
	assert.NotNil(t, mmapIO)
	assert.Nil(t, err)

	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)

	fio, err := NewFileIOManager(path)
	assert.NotNil(t, fio)
	assert.Nil(t, err)

	_, err = fio.Write([]byte("aa"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("bb"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("cc"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("dd"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	size, err := mmapIO2.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(8), size)

	b2 := make([]byte, 2)
	n2, err := mmapIO2.Read(b2, 0)
	t.Log(n2)
	t.Log(string(b2))

	b3 := make([]byte, 2)
	n3, err := mmapIO2.Read(b3, 2)
	t.Log(n3)
	t.Log(string(b3))
}
