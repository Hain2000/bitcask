package cluster

import (
	"bitcask"
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/hashicorp/raft"
	"io"
)

type FSM struct {
	store *bitcask.DB // 持有底层存储
}

func NewFSM(store *bitcask.DB) *FSM {
	return &FSM{store: store}
}

// Apply 应用日志条目
func (f *FSM) Apply(log *raft.Log) interface{} {
	var entry LogEntry
	if err := gob.NewDecoder(bytes.NewReader(log.Data)).Decode(&entry); err != nil {
		return err
	}

	switch entry.Op {
	case OpPut:
		return f.store.Put(entry.Key, entry.Value)
	case OpDelete:
		return f.store.Delete(entry.Key)
	default:
		return errors.New("unknown operation type")
	}
}

// Snapshot 生成快照
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &Snapshot{}, nil
}

// Restore 从快照恢复
func (f *FSM) Restore(rc io.ReadCloser) error {
	return nil
}

// Snapshot 实现
type Snapshot struct{}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	return nil
}

func (s *Snapshot) Release() {}
