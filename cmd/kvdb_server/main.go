package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync" // Import sync package for WaitGroup
	"syscall"

	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/cluster"
	grpc_protocol "github.com/Hain2000/bitcask/protocol/grpc"
	http_protocol "github.com/Hain2000/bitcask/protocol/http"
)

func main() {
	var (
		nodeID    = flag.String("node-id", "node1", "Cluster node ID")
		raftAddr  = flag.String("raft-addr", "127.0.0.1:7000", "Raft bind address")
		grpcPort  = flag.String("grpc-port", "50051", "gRPC server port")
		httpPort  = flag.String("http-port", "8080", "HTTP server port") // Renamed from 'port'
		raftDir   = flag.String("raft-dir", "./raft-data", "Raft data directory (make default consistent)")
		joinAddr  = flag.String("join-addr", "", "Existing cluster member address to join")
		bootstrap = flag.Bool("bootstrap", false, "Bootstrap the initial cluster if set")
	)
	flag.Parse()

	log.Printf("Initializing storage in directory: %s", *raftDir)
	if err := os.MkdirAll(*raftDir, 0755); err != nil && !os.IsExist(err) {
		log.Fatalf("Failed to create Raft data directory %s: %v", *raftDir, err)
	}

	opts := bitcask.DefaultOptions
	opts.DirPath = *raftDir
	store, err := bitcask.Open(opts)
	if err != nil {
		log.Fatalf("Failed to initialize storage engine: %v", err)
	}
	defer func() {
		log.Println("Closing storage engine...")
		if err := store.Close(); err != nil {
			log.Printf("Error closing storage engine: %v", err)
		}
	}()
	log.Println("Storage engine initialized successfully.")

	// Create cluster node configuration
	config := &cluster.Config{
		NodeID:    *nodeID,
		RaftDir:   *raftDir,
		RaftBind:  *raftAddr,
		JoinAddr:  *joinAddr,
		Bootstrap: *bootstrap,
	}

	log.Println("Initializing cluster node...")
	node, err := cluster.NewNode(store, config)
	if err != nil {
		log.Fatalf("Failed to create cluster node: %v", err)
	}
	log.Printf("Cluster node '%s' initialized.", *nodeID)

	grpcServer := grpc_protocol.NewServer(node)
	httpServer := http_protocol.NewServer(node, *httpPort)

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start gRPC Server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("Starting gRPC server on :%s", *grpcPort)
		if err := grpcServer.Start(*grpcPort); err != nil {
			log.Printf("gRPC server failed: %v", err)
			cancel()
		}
		log.Println("gRPC server stopped.")
	}()

	// Start HTTP Server
	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("Starting HTTP server on :%s", *httpPort)
		if err := httpServer.Start(); err != nil && err != http.ErrServerClosed { // Check for ErrServerClosed for graceful shutdown
			log.Printf("HTTP server failed: %v", err)
			cancel()
		}
		log.Println("HTTP server stopped.")
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("Received shutdown signal: %v", sig)
		cancel()
	case <-ctx.Done():
		log.Println("Context cancelled, initiating shutdown...")
	}

	log.Println("Waiting for servers to stop...")
	wg.Wait()

	log.Println("All servers stopped. Exiting.")
}
