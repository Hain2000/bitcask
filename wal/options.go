package wal

import (
	"os"
	"time"
)

type Options struct {
	DirPath              string
	SegmentSize          int64
	SegmentFileExtension string
	Sync                 bool
	BytePerSync          uint32 // 调用fsync之前要写入的字节数
	SyncInterval         time.Duration
}

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

var DefaultOptions = Options{
	DirPath:              os.TempDir(),
	SegmentSize:          GB,
	SegmentFileExtension: ".SEG",
	Sync:                 false,
	BytePerSync:          0,
	SyncInterval:         0,
}
