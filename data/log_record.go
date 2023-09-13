package data

import "encoding/binary"

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
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}

func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, int64) {
	return nil, 0
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	return 0
}
