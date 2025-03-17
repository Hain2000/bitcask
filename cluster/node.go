package cluster

import (
	"fmt"
	"github.com/Hain2000/bitcask"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path/filepath"
	"time"
)

type Node struct {
	Raft          *raft.Raft
	Store         *bitcask.DB
	id            string
	fStateMachine *FSM
	config        *Config
}

type Config struct {
	NodeID    string
	RaftDir   string
	RaftBind  string
	JoinAddr  string
	Bootstrap bool
}

func NewNode(store *bitcask.DB, config *Config) (*Node, error) {
	f := NewFSM(store)
	node := &Node{
		id:            config.NodeID,
		fStateMachine: f,
		Store:         store,
		config:        config,
	}
	if err := node.startRaft(); err != nil {
		return nil, fmt.Errorf("failed to setup raft node: %v", err)
	}
	return node, nil
}

func (node *Node) ID() string {
	return node.id
}

func (node *Node) startRaft() error {
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(node.id)

	addr, err := net.ResolveTCPAddr("tcp", node.config.RaftBind)
	if err != nil {
		return fmt.Errorf("failed to resolve raft address: %v", err)
	}

	transport, err := raft.NewTCPTransport(node.config.RaftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create raft transport: %v", err)
	}

	snapshorts, err := raft.NewFileSnapshotStore(node.config.RaftDir, 3, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create snapshot store: %v", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(node.config.RaftDir, "raft-log.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create raft log.bolt: %v", err)
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(node.config.RaftDir, "raft-stable.bolt"))
	if err != nil {
		return fmt.Errorf("failed to create raft stable.bolt: %v", err)
	}

	rf, err := raft.NewRaft(raftConfig, node.fStateMachine, logStore, stableStore, snapshorts, transport)
	if err != nil {
		return fmt.Errorf("failed to create raft server: %v", err)
	}
	node.Raft = rf
	if node.config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{ID: raftConfig.LocalID, Address: transport.LocalAddr()},
			},
		}
		rf.BootstrapCluster(configuration)
	}
	return nil
}
