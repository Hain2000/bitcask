package data

import (
	"github.com/stretchr/testify/assert"
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

	// deleted
}
