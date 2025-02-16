package raft

import (
	pb "bitcask/raftpb"
	"context"
	"fmt"
	"time"
)

func (rf *Raft) HandleAppendEntries(req *pb.AppendEntriesRequest, resp *pb.AppendEntriesResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.PersistRaftState()

	if req.Term < rf.curTerm {
		resp.Term = rf.curTerm
		resp.Success = false
		return
	}
	// 只要 Leader 的 term 不小于自己，就对其进行认可，变为 Follower
	if req.Term > rf.curTerm {
		rf.curTerm = req.Term
		rf.votedFor = VoteForNoOne
	}

	rf.SwitchRaftNodeRole(NodeRoleFollower)
	rf.leaderId = req.LeaderId
	rf.electionTimer.Reset(time.Duration(MakeAnRandomElectionTimeout(int(rf.baseElectTimeout))) * time.Millisecond)
	resp.Term, resp.Success = rf.curTerm, true
}

func (rf *Raft) replicateOnRound(peer *RaftClientEnd) {
	rf.mu.Lock()
	if rf.role != NodeRoleLeader {
		rf.mu.RUnlock()
	}

	appendEntReq := &pb.AppendEntriesRequest{
		Term:     rf.curTerm,
		LeaderId: int64(rf.me),
	}
	rf.mu.RUnlock()

	resp, err := (*peer.raftServiceCli).AppendEntries(context.Background(), appendEntReq)
	if err != nil {
		PrintDebugLog(fmt.Sprintf("send append entries to %s failed %v\n", peer.addr, err.Error()))
	}

	if resp.Term > rf.curTerm {
		rf.SwitchRaftNodeRole(NodeRoleFollower)
		rf.curTerm = resp.Term
		rf.votedFor = VoteForNoOne
		rf.PersistRaftState()
	}
}
