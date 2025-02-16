package raft

import (
	"bitcask/engine"
	pb "bitcask/raftpb"
	"bytes"
	"encoding/binary"
	"encoding/gob"
)

type RaftPersistentState struct {
	CurTerm  int64
	VotedFor int64
}

func MakePersistRaftLog(eng engine.KvStorageEngine) *RaftLog {
	_, _, err := eng.SeekPrefixFirst(string(RaftLogPrefix))
	if err != nil {
		empEnt := &pb.Entry{}
		empEntEncode := EncodeEntry(empEnt)
		eng.PutBytesKv(EncodeRaftLogKey(InitLogIndex), empEntEncode)
	}
	return &RaftLog{storeEngine: eng}
}

func EncodeEntry(ent *pb.Entry) []byte {
	var bytesEnt bytes.Buffer
	enc := gob.NewEncoder(&bytesEnt)
	enc.Encode(ent)
	return bytesEnt.Bytes()
}

func DecodeEntry(in []byte) *pb.Entry {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	ent := pb.Entry{}
	dec.Decode(&ent)
	return &ent
}

func EncodeRaftLogKey(idx uint64) []byte {
	key := make([]byte, 4+8)
	copy(key[:4], RaftLogPrefix)
	binary.BigEndian.PutUint64(key[4:], idx)
	return key
}

func DecodeRaftLogKey(bts []byte) uint64 {
	return binary.BigEndian.Uint64(bts[4:])
}

func EncodeRaftState(rfState *RaftPersistentState) []byte {
	var bytesState bytes.Buffer
	enc := gob.NewEncoder(&bytesState)
	enc.Encode(rfState)
	return bytesState.Bytes()
}

func DecodeRaftState(in []byte) *RaftPersistentState {
	dec := gob.NewDecoder(bytes.NewBuffer(in))
	rfState := RaftPersistentState{}
	dec.Decode(&rfState)
	return &rfState
}

func (rl *RaftLog) PersistRaftState(curTerm, voteFor int64) {
	rfState := &RaftPersistentState{CurTerm: curTerm, VotedFor: voteFor}
	rl.storeEngine.PutBytesKv(RaftStateKey, EncodeRaftState(rfState))
}

func (rl *RaftLog) ReadRaftState() (curTerm int64, votedFor int64) {
	rfBytes, err := rl.storeEngine.GetBytesValue(RaftStateKey)
	if err != nil {
		return 0, -1
	}
	rfState := DecodeRaftState(rfBytes)
	return rfState.CurTerm, rfState.VotedFor
}
