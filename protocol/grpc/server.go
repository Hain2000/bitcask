package grpc

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/cluster"
	"github.com/Hain2000/bitcask/protocol/grpc/kvdb"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
)

type Server struct {
	kvdb.UnimplementedKVServiceServer
	node *cluster.Node
}

func NewServer(node *cluster.Node) *Server {
	return &Server{node: node}
}

// 启动 gRPC 服务
func (s *Server) Start(port string) error {
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvdb.RegisterKVServiceServer(grpcServer, s)
	reflection.Register(grpcServer)
	log.Printf("gRPC server listening on :%s", port)
	return grpcServer.Serve(lis)
}

// 辅助方法：验证当前节点是否是Leader
func (s *Server) verifyLeader() error {
	if s.node.Raft.State() != raft.Leader {
		leader := s.node.Raft.Leader()
		if leader == "" {
			return errors.New("no leader elected")
		}
		return fmt.Errorf("redirect to leader: %s", leader)
	}
	return nil
}

// 辅助方法：统一提交日志条目
func (s *Server) applyEntry(entry cluster.LogEntry) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return status.Error(codes.Internal, "failed to encode log entry")
	}

	future := s.node.Raft.Apply(buf.Bytes(), 5*time.Second)
	if err := future.Error(); err != nil {
		return status.Error(codes.Aborted, err.Error())
	}
	return nil
}

// 实现 Put 方法
func (s *Server) Put(ctx context.Context, req *kvdb.PutRequest) (*kvdb.PutResponse, error) {
	entry := cluster.LogEntry{
		Op:    cluster.OpPut,
		Key:   req.Key,
		Value: req.Value,
	}

	if err := s.applyEntry(entry); err != nil {
		return &kvdb.PutResponse{Success: false, Error: err.Error()}, nil
	}
	return &kvdb.PutResponse{Success: true}, nil
}

// Get 实现键值查询
func (s *Server) Get(ctx context.Context, req *kvdb.GetRequest) (*kvdb.GetResponse, error) {
	// 确保线性一致性读
	if err := s.verifyLeader(); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	// 从本地状态机读取
	value, err := s.node.Store.Get(req.Key)
	switch {
	case errors.Is(err, bitcask.ErrKeyNotFound):
		return &kvdb.GetResponse{Found: false}, nil
	case err != nil:
		log.Printf("Get operation failed: %v", err)
		return nil, status.Error(codes.Internal, "internal storage error")
	default:
		return &kvdb.GetResponse{
			Value: value,
			Found: true,
		}, nil
	}
}

// Delete 实现键值删除
func (s *Server) Delete(ctx context.Context, req *kvdb.DeleteRequest) (*kvdb.DeleteResponse, error) {
	// 构造删除日志条目
	entry := cluster.LogEntry{
		Op:  cluster.OpDelete,
		Key: req.Key,
	}

	// 提交到Raft集群
	if err := s.applyEntry(entry); err != nil {
		log.Printf("Delete operation failed: %v", err)
		return &kvdb.DeleteResponse{
			Error: err.Error(),
		}, nil
	}

	return &kvdb.DeleteResponse{}, nil
}

func (s *Server) JoinCluster(ctx context.Context, req *kvdb.JoinRequest) (*kvdb.JoinResponse, error) {
	addFuture := s.node.Raft.AddVoter(
		raft.ServerID(req.NodeId),
		raft.ServerAddress(req.RaftAddress),
		0, 0,
	)

	if err := addFuture.Error(); err != nil {
		return &kvdb.JoinResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &kvdb.JoinResponse{Success: true}, nil
}

func (s *Server) ClusterStatus(ctx context.Context, req *kvdb.StatusRequest) (*kvdb.StatusResponse, error) {
	config := s.node.Raft.GetConfiguration().Configuration()

	nodes := make([]*kvdb.Node, 0, len(config.Servers))
	for _, srv := range config.Servers {
		nodes = append(nodes, &kvdb.Node{
			Id:      string(srv.ID),
			Address: string(srv.Address),
		})
	}

	return &kvdb.StatusResponse{
		LeaderId: string(s.node.Raft.Leader()),
		Nodes:    nodes,
	}, nil
}
