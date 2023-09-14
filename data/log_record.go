package data

import (
	"encoding/binary"
	"hash/crc32"
)

// LogRecordPos 内存数据索引，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id: 表示数据存储到哪个文件中去了
	Offset int64  // 偏移量，表示将数据存储到文件的哪个位置
}

type LogRecordType = byte

// maxLogRecordHeaderSize (cyc)4 + (type)1 + (k size)5 + (v size)5
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type LogRecordHeader struct {
	crc        uint32
	recordType LogRecordType
	keySize    uint32
	valueSize  uint32
}

// EncodeLogRecord 转换LogRecord为字符数组和长度
// {log_record[], 长度}
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	// {cyc(4) | type(1) | key_size([0, 5)) | value_size([0, 5)) | key | value}
	header := make([]byte, maxLogRecordHeaderSize)

	// 第5个字节存type
	header[4] = logRecord.Type
	var idx = 5
	idx += binary.PutVarint(header[idx:], int64(len(logRecord.Key)))   // (+ key_size)
	idx += binary.PutVarint(header[idx:], int64(len(logRecord.Value))) // (+ value_size)
	var size = idx + len(logRecord.Value) + len(logRecord.Key)
	eBytes := make([]byte, size)
	// 深拷贝
	copy(eBytes[:idx], header[:idx])
	// 搞定了头后，把kv内容拷过来
	copy(eBytes[idx:], logRecord.Key)
	copy(eBytes[idx+len(logRecord.Key):], logRecord.Value)
	// 进行crc校验
	crc := crc32.ChecksumIEEE(eBytes[4:])          //
	binary.LittleEndian.PutUint32(eBytes[:4], crc) // 小端序

	// fmt.Printf("header size : %d, crc : %d\n", idx, crc)

	return eBytes, int64(size)
}

// decodeLogRecordHeader {头部， 头长}
func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := &LogRecordHeader{
		crc:        binary.LittleEndian.Uint32(buf[:4]),
		recordType: buf[4],
	}

	var idx = 5
	ks, n := binary.Varint(buf[idx:])
	header.keySize = uint32(ks)
	idx += n
	vs, n := binary.Varint(buf[idx:])
	header.valueSize = uint32(vs)
	idx += n

	return header, int64(idx)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, lr.Key)
	crc = crc32.Update(crc, crc32.IEEETable, lr.Value)
	return crc
}
