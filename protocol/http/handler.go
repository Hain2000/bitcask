package http

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/cluster"
	"github.com/hashicorp/raft"
	"log"
	"net/http"
	"time"
)

// PUT操作请求体
type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// 构造日志条目
	entry := cluster.LogEntry{
		Op:    cluster.OpPut,
		Key:   []byte(req.Key),
		Value: []byte(req.Value),
	}

	// 提交到Raft集群
	if err := s.applyEntry(entry); err != nil {
		s.handleRaftError(w, err)
		return
	}

	writeResponse(w, http.StatusOK, "ok")
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method not allowed"))
		return
	}

	var req PutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	// 构造日志条目
	entry := cluster.LogEntry{
		Op:    cluster.OpPut,
		Key:   []byte(req.Key),
		Value: []byte(req.Value),
	}

	// 提交到Raft集群
	if err := s.applyEntry(entry); err != nil {
		s.handleRaftError(w, err)
		return
	}

	writeResponse(w, http.StatusOK, "ok")
}

// GET操作处理
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("missing key parameter"))
		return
	}

	// 直接从本地状态机读取（线性一致性读需先验证Leader）
	value, err := s.node.Store.Get([]byte(key))
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyIsEmpty) {
			writeResponse(w, http.StatusNotFound, nil)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeResponse(w, http.StatusOK, string(value))
}

// 统一提交日志方法
func (s *Server) applyEntry(entry cluster.LogEntry) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return err
	}

	future := s.node.Raft.Apply(buf.Bytes(), 5*time.Second)
	return future.Error()
}

// Raft错误处理
func (s *Server) handleRaftError(w http.ResponseWriter, err error) {
	if errors.Is(err, raft.ErrNotLeader) {
		leader := s.node.Raft.Leader()
		writeError(w, http.StatusTemporaryRedirect, fmt.Errorf("redirect to leader: %s", leader))
		return
	}
	writeError(w, http.StatusInternalServerError, err)
}

// 处理节点加入
func (s *Server) handleJoin(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("处理请求时发生 panic: %v", err)
			writeError(w, http.StatusInternalServerError, fmt.Errorf("内部错误"))
		}
	}()
	log.Println("接收到加入集群请求")

	var req struct {
		NodeID string `json:"node_id"`
		Addr   string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("请求解析失败: %v", err)
		writeError(w, http.StatusBadRequest, err)
		return
	}
	log.Printf("尝试添加节点: ID=%s, Address=%s", req.NodeID, req.Addr)

	configFuture := s.node.Raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 检查节点是否已存在
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(req.NodeID) || srv.Address == raft.ServerAddress(req.Addr) {
			writeError(w, http.StatusConflict, fmt.Errorf("node %s already exists", req.NodeID))
			return
		}
	}

	// 添加新的投票节点
	addFuture := s.node.Raft.AddVoter(
		raft.ServerID(req.NodeID),
		raft.ServerAddress(req.Addr),
		0,
		0,
	)

	if err := addFuture.Error(); err != nil {
		log.Printf("添加节点失败: %v", err) // 关键日志
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	log.Printf("节点 %s 添加成功", req.NodeID)
	writeResponse(w, http.StatusOK, "ok")
}

// 查看集群状态
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"node_id": s.node.ID(),
		"leader":  s.node.Raft.Leader(),
		"state":   s.node.Raft.State().String(),
		"nodes":   s.node.Raft.GetConfiguration().Configuration().Servers,
		"stats":   s.node.Raft.Stats(),
	}
	writeResponse(w, http.StatusOK, status)
}
