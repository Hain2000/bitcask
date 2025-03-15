package resp

import (
	"bitcask"
	bitcaskredis "bitcask/redis"
	"errors"
	"fmt"
	"github.com/tidwall/redcon"
	"strconv"
	"strings"
	"time"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type Handler struct {
	store *bitcaskredis.DataStructure
}

func NewHandler(store *bitcaskredis.DataStructure) *Handler {
	return &Handler{store: store}
}

// HandleCommand 直接使用redcon解析后的命令参数
func (h *Handler) HandleCommand(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	case "set":
		h.handleSet(conn, cmd.Args[1:])
	case "get":
		h.handleGet(conn, cmd.Args[1:])
	case "mset":
		h.handleMSet(conn, cmd.Args[1:])
	case "mget":
		h.handleMGet(conn, cmd.Args[1:])
	case "incrby":
		h.handleIncrBy(conn, cmd.Args[1:], 1)
	case "incr":
		h.handleIncr(conn, cmd.Args[1:])
	case "decr":
		h.handleDecr(conn, cmd.Args[1:])
	case "setnx":
		h.handleSetNX(conn, cmd.Args[1:])
	case "zadd":
		h.handleZAdd(conn, cmd.Args[1:])
	case "zscore":
		h.handleZScore(conn, cmd.Args[1:])
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	}
}

func (h *Handler) handleSet(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'set' command")
		return
	}

	key := args[0]
	value := args[1]
	var ttl time.Duration

	// 解析可选参数
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "EX":
			if i+1 >= len(args) {
				conn.WriteError("ERR syntax error")
				return
			}
			sec, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				conn.WriteError("ERR invalid expire time")
				return
			}
			ttl = time.Duration(sec) * time.Second
			i++ // 跳过已处理的参数
		case "PX":
			if i+1 >= len(args) {
				conn.WriteError("ERR syntax error")
				return
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				conn.WriteError("ERR invalid expire time")
				return
			}
			ttl = time.Duration(ms) * time.Millisecond
			i++
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}

	if err := h.store.Set(key, value, ttl); err != nil {
		conn.WriteError("ERR " + err.Error())
	} else {
		conn.WriteString("OK")
	}
}

// handleGet 处理GET命令
func (h *Handler) handleGet(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'get' command")
		return
	}

	value, err := h.store.Get(args[0])
	switch {
	case errors.Is(err, bitcask.ErrKeyNotFound):
		conn.WriteNull()
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteBulk(value)
	}
}

// handleMSet 处理MSET命令
func (h *Handler) handleMSet(conn redcon.Conn, args [][]byte) {
	if len(args)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for 'mset' command")
		return
	}

	// 转换参数格式
	kvs := make([]string, 0, len(args))
	for _, arg := range args {
		kvs = append(kvs, string(arg))
	}

	count, err := h.store.MSet(kvs)
	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR partial success (%d), errors: %v", count, err))
	} else {
		conn.WriteString("OK")
	}
}

// handleMGet 处理MGET命令
func (h *Handler) handleMGet(conn redcon.Conn, args [][]byte) {
	keys := make([]string, len(args))
	for i, k := range args {
		keys[i] = string(k)
	}

	values, err := h.store.MGet(keys)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	conn.WriteArray(len(values))
	for _, v := range values {
		if v == "" { // 处理不存在的键
			conn.WriteNull()
		} else {
			conn.WriteBulkString(v)
		}
	}
}

// handleIncrBy 处理INCRBY/DECR命令
func (h *Handler) handleIncrBy(conn redcon.Conn, args [][]byte, delta int64) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}

	key := args[0]
	incr, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		conn.WriteError("ERR invalid increment value")
		return
	}

	result, err := h.store.IncrBy(key, incr*delta) // delta用于处理DECR
	if err != nil {
		conn.WriteError("ERR " + err.Error())
	} else {
		conn.WriteInt64(result)
	}
}

func (h *Handler) handleIncr(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'incr' command")
		return
	}

	result, err := h.store.Incr(args[0])
	if err != nil {
		conn.WriteError("ERR " + err.Error())
	} else {
		conn.WriteInt64(result)
	}
}

func (h *Handler) handleDecr(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'decr' command")
		return
	}

	result, err := h.store.Decr(args[0])
	if err != nil {
		conn.WriteError("ERR " + err.Error())
	} else {
		conn.WriteInt64(result)
	}
}

// handleSetNX 处理SETNX命令
func (h *Handler) handleSetNX(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'setnx' command")
		return
	}

	key := args[0]
	value := args[1]

	if exist, err := h.store.SetNX(key, value); err != nil {
		conn.WriteError("ERR " + err.Error())
	} else {
		// 返回1表示设置成功，0表示键已存在
		if exist {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
	}
}

//

func (h *Handler) handleZAdd(conn redcon.Conn, args [][]byte) {
	if len(args) < 3 {
		conn.WriteError("ERR wrong number of arguments for 'zadd'")
		return
	}

	key := args[0]
	score, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		conn.WriteError("ERR invalid score: " + err.Error())
		return
	}
	member := args[2]

	added, err := h.store.ZAdd(key, score, member)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
	} else {
		conn.WriteAny(added)
	}
}

func (h *Handler) handleZScore(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'zscore'")
		return
	}

	key := args[0]
	member := args[1]

	score, err := h.store.ZScore(key, member)
	if err != nil {
		conn.WriteNull()
	} else {
		conn.WriteBulkString(strconv.FormatFloat(score, 'f', -1, 64))
	}
}
