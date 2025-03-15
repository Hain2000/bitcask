package cluster

import "encoding/gob"

// OperationType 定义操作类型
type OperationType byte

const (
	OpPut OperationType = iota
	OpDelete
)

// LogEntry 定义 Raft 日志条目
type LogEntry struct {
	Op    OperationType
	Key   []byte
	Value []byte // 仅 OpPut 有效
}

// 注册类型以便 gob 编解码
func init() {
	gob.Register(LogEntry{})
}
