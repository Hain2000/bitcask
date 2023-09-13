package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	dataFile2, err := OpenDataFile(os.TempDir(), 191)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)
	dataFile3, err := OpenDataFile(os.TempDir(), 191)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
}

func TestFile_Write(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	err = dataFile1.Write([]byte("lcyyyds"))
	assert.Nil(t, err)
	err = dataFile1.Write([]byte("hain"))
	assert.Nil(t, err)
}

func TestFile_Close(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	err = dataFile1.Write([]byte("lcyyyds"))
	assert.Nil(t, err)

	err = dataFile1.Close()
	assert.Nil(t, err)
}

func TestFile_Sync(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 500)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	err = dataFile1.Write([]byte("lcyyyds"))
	assert.Nil(t, err)

	err = dataFile1.Sync()
	assert.Nil(t, err)
}