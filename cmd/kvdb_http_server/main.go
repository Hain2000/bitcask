package main

import (
	"bitcask"
	"bitcask/cluster"
	"bitcask/protocol/http"
	"flag"
	"log"
)

func main() {
	var (
		port      = flag.String("port", "8080", "HTTP server port")
		raftDir   = flag.String("raft-dir", "./raft-data", "Raft data directory")
		nodeID    = flag.String("node-id", "node1", "Cluster node ID")
		raftAddr  = flag.String("raft-addr", "127.0.0.1:7000", "Raft bind address")
		joinAddr  = flag.String("join-addr", "", "Existing cluster member address")
		bootstrap = flag.Bool("bootstrap", false, "Bootstrap initial cluster")
	)
	flag.Parse()

	// 初始化存储引擎
	store, err := bitcask.Open(bitcask.DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	// 创建集群节点
	nodeConfig := &cluster.Config{
		NodeID:    *nodeID,
		RaftDir:   *raftDir,
		RaftBind:  *raftAddr,
		JoinAddr:  *joinAddr,
		Bootstrap: *bootstrap,
	}

	node, err := cluster.NewNode(store, nodeConfig)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	// 启动HTTP服务
	httpServer := http.NewServer(node, *port)
	log.Printf("Server started on :%s", *port)
	log.Fatal(httpServer.Start())
}
