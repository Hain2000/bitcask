package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// normal
	rec1 := &LogRecord{
		Key:   []byte("123456789"),
		Value: []byte("123456789"),
		Type:  LogRecordNormal,
	}
	res1, size1 := EncodeLogRecord(rec1)
	assert.NotNil(t, res1)
	assert.Greater(t, size1, int64(5))
	t.Log(res1)
	t.Log(size1)
	// value empty
	rec2 := &LogRecord{
		Key:  []byte("123456789"),
		Type: LogRecordNormal,
	}
	res2, size2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, size2, int64(5))
	t.Log(res2)
	// deleted
	rec3 := &LogRecord{
		Key:   []byte("123456789"),
		Value: []byte("123456789"),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	assert.NotNil(t, res3)
	assert.Greater(t, size3, int64(5))
	t.Log(res3)
	t.Log(size3)
}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{112, 144, 98, 32, 0, 18, 18}
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	assert.NotNil(t, h1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(543330416), h1.crc)
	assert.Equal(t, LogRecordNormal, h1.recordType)
	assert.Equal(t, uint32(9), h1.keySize)
	assert.Equal(t, uint32(9), h1.valueSize)

	headerBuf2 := []byte{237, 150, 178, 160, 0, 18, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	t.Log(h2)
	t.Log(size2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(2696058605), h2.crc)
	assert.Equal(t, LogRecordNormal, h2.recordType)
	assert.Equal(t, uint32(9), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)

	headerBuf3 := []byte{238, 19, 184, 191, 1, 18, 18}
	h3, size3 := decodeLogRecordHeader(headerBuf3)
	t.Log(h3)
	t.Log(size3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(3216511982), h3.crc)
	assert.Equal(t, LogRecordDeleted, h3.recordType)
	assert.Equal(t, uint32(9), h3.keySize)
	assert.Equal(t, uint32(9), h3.valueSize)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("123456789"),
		Value: []byte("123456789"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{112, 144, 98, 32, 0, 18, 18}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	t.Log(crc1)

	rec2 := &LogRecord{
		Key:  []byte("123456789"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{237, 150, 178, 160, 0, 18, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(2696058605), crc2)
	t.Log(crc2)

	rec3 := &LogRecord{
		Key:   []byte("123456789"),
		Value: []byte("123456789"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{238, 19, 184, 191, 1, 18, 18}
	crc3 := getLogRecordCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(3216511982), crc3)

}
