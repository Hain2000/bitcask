package bitcask

import "os"

type Options struct {
	DirPath            string // 数据库数据目录
	DataFileSize       int64  // 数据文件的大小
	SyncWrite          bool   // 每次写是否需要持久化
	IndexType          IdxType
	BytesPerSync       uint // 累计写该阈值时，进行持久化
	MMapAtStartup      bool // 启动MMap
	DataFileMergeRatio float32
}

type IdxType = int8

const (
	BTREE IdxType = iota + 1
	ART
	BPLUSTREE
)

var DefaultOptions = Options{
	DirPath:            os.TempDir(),
	DataFileSize:       256 * 1024 * 1024, // 256MB
	SyncWrite:          false,
	BytesPerSync:       0,
	IndexType:          ART,
	MMapAtStartup:      true,
	DataFileMergeRatio: 0.5,
}

// IteratorOptions 迭代器设置
type IteratorOptions struct {
	Prefix  []byte // 遍历前缀指定的Key，默认为空
	Reverse bool   // 是否反向，默认false(正向)
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

type WriteBatchOptions struct {
	MaxBatchNum uint // 一个批次中最大的数据量
	SyncWrites  bool // 提交时是否sync持久化
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
