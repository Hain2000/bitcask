package main

import (
	"context"
	"flag"
	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/cluster"
	"github.com/Hain2000/bitcask/protocol/grpc"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 解析命令行参数
	var (
		nodeID    = flag.String("node-id", "node1", "Cluster node ID")
		raftAddr  = flag.String("raft-addr", "127.0.0.1:7000", "Raft bind address")
		grpcPort  = flag.String("grpc-port", "50051", "gRPC server port")
		raftDir   = flag.String("raft-dir", "../tmp/raft-data", "Raft data directory")
		joinAddr  = flag.String("join-addr", "", "Existing cluster member address")
		bootstrap = flag.Bool("bootstrap", false, "Bootstrap initial cluster")
	)

	flag.Parse()

	// 初始化存储引擎
	opts := bitcask.DefaultOptions
	opts.DirPath = *raftDir
	store, err := bitcask.Open(opts)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("Error closing store: %v", err)
		}
	}()

	// 创建集群节点配置
	config := &cluster.Config{
		NodeID:    *nodeID,
		RaftDir:   *raftDir,
		RaftBind:  *raftAddr,
		JoinAddr:  *joinAddr,
		Bootstrap: *bootstrap,
	}

	// 初始化集群节点
	node, err := cluster.NewNode(store, config)
	if err != nil {
		log.Fatalf("Failed to create cluster node: %v", err)
	}

	// 创建 gRPC 服务器
	grpcServer := grpc.NewServer(node)

	// 创建带取消功能的上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动 gRPC 服务器
	go func() {
		log.Printf("Starting gRPC server on :%s", *grpcPort)
		if err := grpcServer.Start(*grpcPort); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	// 等待中断信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("Received shutdown signal")
	case <-ctx.Done():
		log.Println("Context cancelled")
	}

}
