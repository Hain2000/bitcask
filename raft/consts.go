package raft

const (
	VoteForNoOne = -1
)

var RaftLogPrefix = []byte{0x11, 0x11, 0x19, 0x96}

var FirstIdxKey = []byte{0x88, 0x88}

var LastIdxKey = []byte{0x99, 0x99}

var RaftStateKey = []byte{0x19, 0x49}

const InitLogIndex = 0

var SnapshotStateKey = []byte{0x19, 0x97}
