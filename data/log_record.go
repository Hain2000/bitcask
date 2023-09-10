package data

// LogRecordPos 内存数据索引，主要描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件 id: 表示数据存储到哪个文件中去了
	Offset int64  // 偏移量，表示将数据存储到文件的哪个位置
}

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
)

type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// EncodeLogRecord 转换LogRecord为字符数组和长度
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
