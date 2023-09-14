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

func TestFile_GetLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 800)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	rec1 := &LogRecord{
		Key:   []byte("hain"),
		Value: []byte("114514"),
		Type:  LogRecordNormal,
	}
	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)
	getRec1, getSize1, err := dataFile.GetLogRecord(0)

	assert.Nil(t, err)
	assert.Equal(t, rec1, getRec1)
	assert.Equal(t, size1, getSize1)
}
