package raft

import (
	"bitcask/engine"
	pb "bitcask/raftpb"
	"sync"
)

type RaftLog struct {
	mu          sync.RWMutex
	storeEngine engine.KvStorageEngine
	items       []*pb.Entry
}

func MakeMemRaftLog() *RaftLog {
	empEnt := &pb.Entry{}
	newItems := []*pb.Entry{}
	newItems = append(newItems, empEnt)
	return &RaftLog{items: newItems}
}
