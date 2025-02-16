package raft

import (
	"bitcask/raftpb"
	"fmt"
	"google.golang.org/grpc"
)

type RaftClientEnd struct {
	id             uint64
	addr           string
	conns          []*grpc.ClientConn
	raftServiceCli *raftpb.RaftServiceClient
}

func (rc *RaftClientEnd) Id() uint64 {
	return rc.id
}

func MakeRaftClientEnd(addr string, id uint64) *RaftClientEnd {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("faild to connect: %v", err)
	}
	conns := []*grpc.ClientConn{}
	conns = append(conns, conn)
	rpcCli := raftpb.NewRaftServiceClient(conn)
	return &RaftClientEnd{
		id:             id,
		addr:           addr,
		conns:          conns,
		raftServiceCli: &rpcCli,
	}
}

func (rc *RaftClientEnd) CloseAllConn() {
	for _, conn := range rc.conns {
		_ = conn.Close()
	}
}
