package raft

import (
	"bitcask/engine"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type NodeRole uint8

const (
	NodeRoleLeader NodeRole = iota
	NodeRoleCandidate
	NodeRoleFollower
)

func NodeToString(role NodeRole) string {
	switch role {
	case NodeRoleLeader:
		return "Leader"
	case NodeRoleCandidate:
		return "Candidate"
	case NodeRoleFollower:
		return "Follower"
	}
	return "unknown"
}

type Raft struct {
	mu    sync.RWMutex
	peers []*RaftClientEnd
	me    int
	dead  int32

	role         NodeRole
	curTerm      int64
	votedFor     int64
	grantedVotes int

	logs      *RaftLog
	persister *RaftLog

	leaderId         int64
	electionTimer    *time.Timer
	heartbeatTimer   *time.Timer
	heartBeatTimeout uint64
	baseElectTimeout uint64
}

func MakeRaft(peers []*RaftClientEnd, me int, storeEngine engine.KvStorageEngine, heartbeatTimeOutMs, baseElectionTimeOutMs uint64) *Raft {
	rf := &Raft{
		peers:            peers,
		me:               me,
		dead:             0,
		role:             NodeRoleFollower,
		curTerm:          0,
		votedFor:         VoteForNoOne,
		grantedVotes:     0,
		logs:             MakePersistRaftLog(storeEngine),
		persister:        MakePersistRaftLog(storeEngine),
		heartbeatTimer:   time.NewTimer(time.Duration(heartbeatTimeOutMs) * time.Millisecond),
		electionTimer:    time.NewTimer(time.Duration(MakeAnRandomElectionTimeout(int(baseElectionTimeOutMs))) * time.Millisecond),
		heartBeatTimeout: heartbeatTimeOutMs,
		baseElectTimeout: baseElectionTimeOutMs,
	}
	rf.curTerm, rf.votedFor = rf.persister.ReadRaftState()
	go rf.Tick()
	return rf
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) IsKilled() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) SwitchRaftNodeRole(role NodeRole) {
	if role == rf.role {
		return
	}
	rf.role = role
	fmt.Printf("note change state to -> %s \n", NodeToString(role))
	switch role {
	case NodeRoleFollower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(time.Duration(MakeAnRandomElectionTimeout(int(rf.baseElectTimeout))) * time.Millisecond)
	case NodeRoleCandidate:
	case NodeRoleLeader:
		rf.leaderId = int64(rf.me)
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(time.Duration(rf.heartBeatTimeout) * time.Millisecond)
	}
}

func (rf *Raft) IncrCurrentTerm() {
	rf.curTerm++
}

func (rf *Raft) IncrGrantedVotes() {
	rf.grantedVotes++
}

func (rf *Raft) PersistRaftState() {
	rf.persister.PersistRaftState(rf.curTerm, rf.votedFor)
}

func (rf *Raft) Tick() {
	for !rf.IsKilled() {
		select {
		case <-rf.electionTimer.C:
			{
				rf.SwitchRaftNodeRole(NodeRoleCandidate)
				rf.IncrCurrentTerm()
				rf.Election()
				rf.electionTimer.Reset(time.Millisecond * time.Duration(MakeAnRandomElectionTimeout(int(rf.baseElectTimeout))))
			}
		case <-rf.heartbeatTimer.C:
			{
				if rf.role == NodeRoleLeader {
					rf.BroadcaseHeartbeat()
					rf.heartbeatTimer.Reset(time.Duration(rf.heartBeatTimeout) * time.Millisecond)
				}
			}
		}
	}
}
