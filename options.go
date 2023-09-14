package bitcask

import "os"

type Options struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64  // 数据文件的大小
	SyncWrite    bool   // 每次写是否需要持久化
	IndexType    IdxType
}

type IdxType = int8

const (
	BTREE IdxType = iota + 1
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrite:    false,
	IndexType:    BTREE,
}
