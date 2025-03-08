package bitcask

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

type Options struct {
	DirPath           string // 数据库数据目录
	SegmentSize       int64
	Sync              bool
	BytePerSync       uint32 // 超过BytePerSync字节时，调用fsync()刷盘
	AutoMergeCronExpr string // 自动触发merge
}

var DefaultOptions = Options{
	DirPath:     tempDBDir(),
	SegmentSize: 1 * GB,
	Sync:        false,
	BytePerSync: 0,
}

// IteratorOptions 迭代器设置
type IteratorOptions struct {
	Prefix          []byte // 遍历前缀指定的Key，默认为空
	Reverse         bool   // 是否反向，默认false(正向)
	ContinueOnError bool
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:          nil,
	Reverse:         false,
	ContinueOnError: false,
}

type BatchOptions struct {
	ReadOnly bool
	Sync     bool // 提交时是否sync持久化
}

var DefaultBatchOptions = BatchOptions{
	ReadOnly: false,
	Sync:     true,
}

var nameRand = rand.NewSource(time.Now().UnixNano())

func tempDBDir() string {
	return filepath.Join(os.TempDir(), "bitcask-temp"+strconv.Itoa(int(nameRand.Int63())))
}
