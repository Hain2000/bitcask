package raft

import (
	pb "bitcask/raftpb"
	"context"
	"fmt"
	"time"
)

func (rf *Raft) HandleRequestVote(req *pb.RequestVoteRequest, resp *pb.RequestVoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.PersistRaftState()

	if req.Term < rf.curTerm || (req.Term == rf.curTerm && rf.votedFor == -1 && rf.votedFor != req.CandidateId) {
		resp.Term, resp.VoteGranted = rf.curTerm, false
		return
	}

	if req.Term > rf.curTerm {
		rf.SwitchRaftNodeRole(NodeRoleFollower)
		rf.curTerm, rf.votedFor = req.CandidateId, -1
	}

	rf.votedFor = req.CandidateId
	rf.electionTimer.Reset(time.Duration(MakeAnRandomElectionTimeout(int(rf.baseElectTimeout))) * time.Millisecond)
	resp.Term, resp.VoteGranted = rf.curTerm, true
}

func (rf *Raft) BroadcaseHeartbeat() {
	for _, peer := range rf.peers {
		if int(peer.Id()) == rf.me {
			continue
		}
		PrintDebugLog(fmt.Sprintf("send heart beat to %s", peer.addr))
		go func(peer *RaftClientEnd) {
			rf.replicateOnRound(peer)
		}(peer)
	}
}

func (rf *Raft) Election() {
	fmt.Printf("%d start election \n", rf.me)
	rf.IncrGrantedVotes()
	rf.votedFor = int64(rf.me)
	voteReqest := &pb.RequestVoteRequest{
		Term:        rf.curTerm,
		CandidateId: int64(rf.me),
	}
	rf.PersistRaftState()
	for _, peer := range rf.peers {
		if int(peer.Id()) == rf.me {
			continue
		}
		go func(peer *RaftClientEnd) {
			PrintDebugLog(fmt.Sprintf("send request vote to %s %s\n", peer.addr, voteReqest.String()))
			requestVoteResp, err := (*peer.raftServiceCli).RequestVote(context.Background(), voteReqest)
			if err != nil {
				PrintDebugLog(fmt.Sprintf("send request vote to %s failed %v\n", peer.addr, err.Error()))
			}
			if requestVoteResp != nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				PrintDebugLog(fmt.Sprintf("send request vote to %s recive -> %s, curterm %d, req term %d", peer.addr, requestVoteResp.String(), rf.curTerm, voteReqest.Term))
				if rf.curTerm == voteReqest.Term && rf.role == NodeRoleCandidate {
					if requestVoteResp.VoteGranted {
						PrintDebugLog("I grant vote")
						rf.IncrGrantedVotes()
						if rf.grantedVotes > len(rf.peers)/2 {
							PrintDebugLog(fmt.Sprintf("node %d get majority votes int term %d ", rf.me, rf.curTerm))
							rf.SwitchRaftNodeRole(NodeRoleLeader)
							rf.BroadcaseHeartbeat()
							rf.grantedVotes = 0
						} else if requestVoteResp.Term > rf.curTerm {
							rf.SwitchRaftNodeRole(NodeRoleFollower)
							rf.curTerm, rf.votedFor = requestVoteResp.Term, VoteForNoOne
							rf.PersistRaftState()
						}
					}
				}
			}
		}(peer)
	}
}
