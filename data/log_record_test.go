package data

import (
	"github.com/stretchr/testify/assert"
	"github.com/valyala/bytebufferpool"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("123456789"),
		Value: []byte("123456789"),
		Type:  LogRecordNormal,
	}
	header := make([]byte, MaxLogRecordHeaderSize)
	buf := bytebufferpool.Get()
	encodeRec := EncodeLogRecord(rec1, header, buf)
	rec2 := DecodeLogRecord(encodeRec)
	assert.Equal(t, rec1, rec2)
	t.Log(rec1)
	t.Log(rec2)
}
