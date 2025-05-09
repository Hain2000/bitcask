package http

import (
	"encoding/json"
	"errors"
	"github.com/Hain2000/bitcask/cluster"
	"log"
	"net/http"
	"time"
)

type Server struct {
	node *cluster.Node
	port string
}

func NewServer(node *cluster.Node, port string) *Server {
	return &Server{node: node, port: port}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	// 键值操作接口
	mux.HandleFunc("/put", s.handlePut)
	mux.HandleFunc("/get", s.handleGet)
	mux.HandleFunc("/delete", s.handleDelete)

	// 集群管理接口
	mux.HandleFunc("/cluster/join", s.handleJoin)
	mux.HandleFunc("/cluster/status", s.handleStatus)

	listenAddr := ":" + s.port

	server := &http.Server{
		Addr:         listenAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	log.Printf("[HTTP Server] Attempting to listen on address: %s", listenAddr)
	err := server.ListenAndServe()

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("[HTTP Server] ListenAndServe returned error: %v", err)
	} else if errors.Is(err, http.ErrServerClosed) {
		log.Println("[HTTP Server] ListenAndServe closed normally.")
	}

	return err
}

// 公共响应格式
func writeResponse(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":  data,
		"error": nil,
	})
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":  nil,
		"error": err.Error(),
	})
}
