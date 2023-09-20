package data

import (
	"bitcask/fio"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	dataFile2, err := OpenDataFile(os.TempDir(), 191, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)
	dataFile3, err := OpenDataFile(os.TempDir(), 191, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile3)
}

func TestFile_Write(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	err = dataFile1.Write([]byte("lcyyyds"))
	assert.Nil(t, err)
	err = dataFile1.Write([]byte("hain"))
	assert.Nil(t, err)
}

func TestFile_Close(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 0, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	err = dataFile1.Write([]byte("lcyyyds"))
	assert.Nil(t, err)

	err = dataFile1.Close()
	assert.Nil(t, err)
}

func TestFile_Sync(t *testing.T) {
	dataFile1, err := OpenDataFile(os.TempDir(), 500, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)
	err = dataFile1.Write([]byte("lcyyyds"))
	assert.Nil(t, err)

	err = dataFile1.Sync()
	assert.Nil(t, err)
}

func TestFile_GetLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 900, fio.StanderFIO)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	rec1 := &LogRecord{
		Key:   []byte("hain"),
		Value: []byte("114514"),
		Type:  LogRecordNormal,
	}
	ans1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(ans1)
	assert.Nil(t, err)
	getRec1, getSize1, err := dataFile.GetLogRecord(0)

	assert.Nil(t, err)
	assert.Equal(t, rec1, getRec1)
	assert.Equal(t, size1, getSize1)

	rec2 := &LogRecord{
		Key:   []byte("hain"),
		Value: []byte("1919810"),
	}
	ans2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(ans2)
	assert.Nil(t, err)

	getRec2, getSize2, err := dataFile.GetLogRecord(size1)
	assert.Nil(t, err)
	assert.Equal(t, rec2, getRec2)
	assert.Equal(t, size2, getSize2)

	rec3 := &LogRecord{
		Key:   []byte("y"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	ans3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(ans3)
	assert.Nil(t, err)

	getRec3, getSize3, err := dataFile.GetLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, rec3, getRec3)
	assert.Equal(t, size3, getSize3)
}
