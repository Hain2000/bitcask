package engine

import (
	"bytes"
	"encoding/binary"

	"os"

	"testing"
)

func TestPrefixRange(t *testing.T) {
	engine, err := MakeStorageEngine("./test_data")
	if err != nil {
		t.Log(err)
		return
	}
	prefixBytes := []byte{0x11, 0x11, 0x19, 0x96}
	for i := 0; i < 300; i++ {
		var outBuf bytes.Buffer
		outBuf.Write(prefixBytes)
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(i))
		outBuf.Write(b)
		t.Logf("write %v", outBuf.Bytes())
		engine.PutBytesKv(outBuf.Bytes(), []byte{byte(i)})
	}

	idMax, err := engine.SeekPrefixKeyIdMax(prefixBytes)
	if err != nil {
		t.Log(err)
		return
	}
	t.Logf("idMax -> %d", idMax)
	engine.db.Close()
	_ = os.RemoveAll("./test_data")
}
