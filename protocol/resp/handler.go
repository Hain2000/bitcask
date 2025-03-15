package resp

import (
	"bitcask"
	bitcaskredis "bitcask/redis"
	"errors"
	"fmt"
	"github.com/tidwall/redcon"
	"math"
	"strconv"
	"strings"
	"time"
)

type Handler struct {
	store *bitcaskredis.DataStructure
}

func NewHandler(store *bitcaskredis.DataStructure) *Handler {
	return &Handler{store: store}
}

// HandleCommand 直接使用redcon解析后的命令参数
func (h *Handler) HandleCommand(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToLower(string(cmd.Args[0])) {
	case "del":
		h.handleDel(conn, cmd.Args[1:])
	case "type":
		h.handleType(conn, cmd.Args[1:])
	case "ttl":
		h.handleTTL(conn, cmd.Args[1:])
	case "expire":
		h.handleExpire(conn, cmd.Args[1:])
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
	case "hset":
		h.handleHSet(conn, cmd.Args[1:])
	case "hget":
		h.handleHGet(conn, cmd.Args[1:])
	case "hdel":
		h.handleHDel(conn, cmd.Args[1:])
	case "hmset":
		h.handleHMSet(conn, cmd.Args[1:])
	case "hmget":
		h.handleHMGet(conn, cmd.Args[1:])
	case "hexists":
		h.handleHExists(conn, cmd.Args[1:])
	case "hkeys":
		h.handleHKeys(conn, cmd.Args[1:])
	case "hvals":
		h.handleHVals(conn, cmd.Args[1:])
	case "hgetall":
		h.handleHGetAll(conn, cmd.Args[1:])
	case "hincrby":
		h.handleHIncrBy(conn, cmd.Args[1:])
	case "hsetnx":
		h.handleHSetNX(conn, cmd.Args[1:])
	case "lpush":
		h.handleLPush(conn, cmd.Args[1:])
	case "rpush":
		h.handleRPush(conn, cmd.Args[1:])
	case "lpop":
		h.handleLPop(conn, cmd.Args[1:])
	case "rpop":
		h.handleRPop(conn, cmd.Args[1:])
	case "lrange":
		h.handleLRange(conn, cmd.Args[1:])
	case "ltrim":
		h.handleLTrim(conn, cmd.Args[1:])
	case "llen":
		h.handleLLen(conn, cmd.Args[1:])
	case "blpop":
		h.handleBLPop(conn, cmd.Args[1:])
	case "brpop":
		h.handleBRPop(conn, cmd.Args[1:])
	case "linsert":
		h.handleLInsert(conn, cmd.Args[1:])
	case "sadd":
		h.handleSAdd(conn, cmd.Args[1:])
	case "sismember":
		h.handleSIsMember(conn, cmd.Args[1:])
	case "srem":
		h.handleSRem(conn, cmd.Args[1:])
	case "scard":
		h.handleSCard(conn, cmd.Args[1:])
	case "smembers":
		h.handleSMembers(conn, cmd.Args[1:])
	case "sinter":
		h.handleSInter(conn, cmd.Args[1:])
	case "sdiff":
		h.handleSDiff(conn, cmd.Args[1:])
	case "sunion":
		h.handleSUnion(conn, cmd.Args[1:])
	case "zadd":
		h.handleZAdd(conn, cmd.Args[1:])
	case "zrem":
		h.handleZRem(conn, cmd.Args[1:])
	case "zscore":
		h.handleZScore(conn, cmd.Args[1:])
	case "zcard":
		h.handleZCard(conn, cmd.Args[1:])
	case "zrange":
		h.handleZRange(conn, cmd.Args[1:], false)
	case "zrevrange":
		h.handleZRange(conn, cmd.Args[1:], true)
	case "zrangebyscore":
		h.handleZRangeByScore(conn, cmd.Args[1:], false)
	case "zrevrangebyscore":
		h.handleZRangeByScore(conn, cmd.Args[1:], true)
	case "zrank":
		h.handleZRank(conn, cmd.Args[1:], false)
	case "zrevrank":
		h.handleZRank(conn, cmd.Args[1:], true)
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	}
}

// generic

func (h *Handler) handleDel(conn redcon.Conn, args [][]byte) {
	if len(args) < 1 {
		conn.WriteError("ERR wrong number of arguments for 'del' command")
		return
	}

	var deleted int64
	for _, key := range args {
		err := h.store.Del(key)
		if err == nil { // 假设 Del 成功返回 nil，即使 key 不存在也视为成功
			deleted++
		}
	}
	conn.WriteInt64(deleted)
}

type redisType = byte

const (
	String redisType = iota
	Hash
	Set
	List
	ZSet
)

func (h *Handler) handleType(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'type' command")
		return
	}

	key := args[0]
	redisType, err := h.store.Type(key)
	if err != nil {
		// 假设当 key 不存在时返回 "none"
		conn.WriteBulkString("none")
		return
	}

	// 根据 redisType 转换为 Redis 协议的类型字符串
	var typeStr string
	switch redisType {
	case String:
		typeStr = "string"
	case Hash:
		typeStr = "hash"
	case List:
		typeStr = "list"
	case Set:
		typeStr = "set"
	case ZSet:
		typeStr = "zset"
	default:
		typeStr = "none"
	}
	conn.WriteBulkString(typeStr)
}

func (h *Handler) handleTTL(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'ttl' command")
		return
	}

	key := args[0]
	// 先检查 key 是否存在
	_, err := h.store.Type(key)
	if err != nil {
		// key 不存在
		conn.WriteInt(-2)
		return
	}

	// 获取 TTL
	ttlSeconds, err := h.store.TTL(key)
	if err != nil {
		// 没有设置 TTL
		conn.WriteInt(-1)
		return
	}

	// 返回整数秒（向下取整）
	conn.WriteInt(int(math.Floor(ttlSeconds)))
}

func (h *Handler) handleExpire(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'expire' command")
		return
	}

	key := args[0]
	// 解析过期时间（秒）
	seconds, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil || seconds < 0 {
		conn.WriteError("ERR invalid expire time")
		return
	}

	// 设置 TTL
	ttl := time.Duration(seconds) * time.Second
	err = h.store.Expire(key, ttl)
	if err != nil {
		// key 不存在或设置失败
		conn.WriteInt(0)
		return
	}
	// 成功返回 1
	conn.WriteInt(1)
}

// string

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

// hash

func (h *Handler) handleHSet(conn redcon.Conn, args [][]byte) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for 'hset' command")
		return
	}

	key := args[0]
	var added int64

	// 遍历 field-value 对
	for i := 1; i < len(args); i += 2 {
		field := args[i]
		value := args[i+1]

		ok, err := h.store.HSet(key, field, value)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if ok {
			added++
		}
	}
	conn.WriteInt64(added)
}

func (h *Handler) handleHGet(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'hget' command")
		return
	}

	key := args[0]
	field := args[1]

	value, err := h.store.HGet(key, field)
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			conn.WriteNull()
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	if value == nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(value)
	}
}

func (h *Handler) handleHDel(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'hdel' command")
		return
	}

	key := args[0]
	var deleted int64

	// 遍历 fields
	for _, field := range args[1:] {
		ok, err := h.store.HDel(key, field)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if ok {
			deleted++
		}
	}
	conn.WriteInt64(deleted)
}

func (h *Handler) handleHMSet(conn redcon.Conn, args [][]byte) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for 'hmset' command")
		return
	}

	key := args[0]
	fields := make(map[string]string)

	// 构建 field-value 字典
	for i := 1; i < len(args); i += 2 {
		field := string(args[i])
		value := string(args[i+1])
		fields[field] = value
	}

	count, err := h.store.HMSet(key, fields)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	// Redis HMSET 返回 "OK"
	conn.WriteInt(count)
}

func (h *Handler) handleHMGet(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'hmget' command")
		return
	}

	key := args[0]
	var fields []string

	// 提取 fields
	for _, fieldBytes := range args[1:] {
		fields = append(fields, string(fieldBytes))
	}

	result, err := h.store.HMGet(key, fields...)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	// 构造响应数组
	conn.WriteArray(len(fields))
	for _, field := range fields {
		val := result[field]
		if val == "" {
			conn.WriteNull()
		} else {
			conn.WriteBulkString(val)
		}
	}
}

func (h *Handler) handleHExists(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'hexists' command")
		return
	}

	key := args[0]
	field := args[1]

	exist, err := h.store.HExist(key, field)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	if exist {
		conn.WriteInt(1)
	} else {
		conn.WriteInt(0)
	}
}

func (h *Handler) handleHKeys(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'hkeys' command")
		return
	}

	key := args[0]
	keys, err := h.store.HKeys(key)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulkString(k)
	}
}

func (h *Handler) handleHVals(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'hvals' command")
		return
	}

	key := args[0]
	vals, err := h.store.HVals(key)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	conn.WriteArray(len(vals))
	for _, v := range vals {
		conn.WriteBulkString(v)
	}
}

func (h *Handler) handleHGetAll(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'hgetall' command")
		return
	}

	key := args[0]
	result, err := h.store.HGetAll(key)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	// 返回成对 field-value
	conn.WriteArray(len(result) * 2)
	for field, value := range result {
		conn.WriteBulkString(field)
		conn.WriteBulkString(value)
	}
}

func (h *Handler) handleHIncrBy(conn redcon.Conn, args [][]byte) {
	if len(args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'hincrby' command")
		return
	}

	key := args[0]
	field := args[1]
	incr, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		conn.WriteError("ERR invalid increment value")
		return
	}

	res, err := h.store.HIncrBy(key, field, incr)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	conn.WriteInt64(res)
}

func (h *Handler) handleHSetNX(conn redcon.Conn, args [][]byte) {
	if len(args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'hsetnx' command")
		return
	}

	key := args[0]
	field := args[1]
	value := args[2]

	ok, err := h.store.HSetNX(key, field, value)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	if ok {
		conn.WriteInt(1)
	} else {
		conn.WriteInt(0)
	}
}

// list

func (h *Handler) handleLPush(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'lpush' command")
		return
	}

	key := args[0]
	elements := args[1:]
	var totalAdded uint32

	for _, element := range elements {
		count, err := h.store.LPush(key, element)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		totalAdded = count
	}

	conn.WriteInt(int(totalAdded))
}

// 处理RPUSH命令（实现类似LPUSH）
func (h *Handler) handleRPush(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'rpush' command")
		return
	}

	key := args[0]
	elements := args[1:]
	var totalAdded uint32

	for _, element := range elements {
		count, err := h.store.RPush(key, element)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		totalAdded = count
	}

	conn.WriteInt(int(totalAdded))
}

// 处理LPOP命令
func (h *Handler) handleLPop(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'lpop' command")
		return
	}

	val, err := h.store.LPop(args[0])
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	case val == nil:
		conn.WriteNull()
	default:
		conn.WriteBulk(val)
	}
}

// 处理RPOP命令（类似LPOP）
func (h *Handler) handleRPop(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'rpop' command")
		return
	}

	val, err := h.store.RPop(args[0])
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	case val == nil:
		conn.WriteNull()
	default:
		conn.WriteBulk(val)
	}
}

// 处理LRANGE命令
func (h *Handler) handleLRange(conn redcon.Conn, args [][]byte) {
	if len(args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'lrange' command")
		return
	}

	key := args[0]
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}

	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}

	values, err := h.store.LRange(key, start, end)
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteArray(len(values))
		for _, v := range values {
			if v == "" {
				conn.WriteNull()
			} else {
				conn.WriteBulkString(v)
			}
		}
	}
}

// 处理LTRIM命令
func (h *Handler) handleLTrim(conn redcon.Conn, args [][]byte) {
	if len(args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'ltrim' command")
		return
	}

	key := args[0]
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}

	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		conn.WriteError("ERR value is not an integer or out of range")
		return
	}

	err = h.store.LTrim(key, start, end)
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteString("OK")
	}
}

// 处理LLEN命令
func (h *Handler) handleLLen(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'llen' command")
		return
	}

	length, err := h.store.LLen(args[0])
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteInt(length)
	}
}

// 处理BLPOP命令（带超时阻塞）
func (h *Handler) handleBLPop(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'blpop' command")
		return
	}

	timeout, err := strconv.ParseFloat(string(args[len(args)-1]), 64)
	if err != nil {
		conn.WriteError("ERR timeout is not a float")
		return
	}

	keys := args[:len(args)-1]
	resultCh := make(chan []byte)
	errorCh := make(chan error)

	// 启动goroutine处理阻塞操作
	go func() {
		for _, key := range keys {
			val, err := h.store.BLPop(key, time.Duration(timeout*float64(time.Second)))
			if err != nil {
				errorCh <- err
				return
			}
			if val != nil {
				resultCh <- val
				return
			}
		}
		errorCh <- nil
	}()

	select {
	case val := <-resultCh:
		conn.WriteArray(2)
		conn.WriteBulk(args[0]) // 返回键名
		conn.WriteBulk(val)
	case err := <-errorCh:
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else {
			conn.WriteNull()
		}
	case <-time.After(time.Duration(timeout * float64(time.Second))):
		conn.WriteNull()
	}
}

func (h *Handler) handleBRPop(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'blpop' command")
		return
	}

	timeout, err := strconv.ParseFloat(string(args[len(args)-1]), 64)
	if err != nil {
		conn.WriteError("ERR timeout is not a float")
		return
	}

	keys := args[:len(args)-1]
	resultCh := make(chan []byte)
	errorCh := make(chan error)

	// 启动goroutine处理阻塞操作
	go func() {
		for _, key := range keys {
			val, err := h.store.BRPop(key, time.Duration(timeout*float64(time.Second)))
			if err != nil {
				errorCh <- err
				return
			}
			if val != nil {
				resultCh <- val
				return
			}
		}
		errorCh <- nil
	}()

	select {
	case val := <-resultCh:
		conn.WriteArray(2)
		conn.WriteBulk(args[0]) // 返回键名
		conn.WriteBulk(val)
	case err := <-errorCh:
		if err != nil {
			conn.WriteError("ERR " + err.Error())
		} else {
			conn.WriteNull()
		}
	case <-time.After(time.Duration(timeout * float64(time.Second))):
		conn.WriteNull()
	}
}

// 处理LINSERT命令
func (h *Handler) handleLInsert(conn redcon.Conn, args [][]byte) {
	if len(args) != 4 {
		conn.WriteError("ERR wrong number of arguments for 'linsert' command")
		return
	}

	key := args[0]
	position := strings.ToUpper(string(args[1]))
	pivot := args[2]
	value := args[3]

	var before bool
	switch position {
	case "BEFORE":
		before = true
	case "AFTER":
		before = false
	default:
		conn.WriteError("ERR syntax error")
		return
	}

	inserted, err := h.store.LInsert(key, pivot, value, before)
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		if inserted {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
	}
}

// set

// 处理SADD命令
func (h *Handler) handleSAdd(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'sadd' command")
		return
	}

	key := args[0]
	members := args[1:]
	added := 0

	for _, member := range members {
		ok, err := h.store.SAdd(key, member)
		if err != nil {
			if errors.Is(err, bitcaskredis.ErrWrongTypeOperation) {
				conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			} else {
				conn.WriteError("ERR " + err.Error())
			}
			return
		}
		if ok {
			added++
		}
	}

	conn.WriteInt(added)
}

// 处理SISMEMBER命令
func (h *Handler) handleSIsMember(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'sismember' command")
		return
	}

	key := args[0]
	member := args[1]

	exists, err := h.store.SIsMember(key, member)
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		if exists {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
	}
}

// 处理SREM命令
func (h *Handler) handleSRem(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'srem' command")
		return
	}

	key := args[0]
	members := args[1:]
	removed := 0

	for _, member := range members {
		ok, err := h.store.SRem(key, member)
		if err != nil {
			if errors.Is(err, bitcaskredis.ErrWrongTypeOperation) {
				conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
			} else {
				conn.WriteError("ERR " + err.Error())
			}
			return
		}
		if ok {
			removed++
		}
	}

	conn.WriteInt(removed)
}

// 处理SCARD命令
func (h *Handler) handleSCard(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'scard' command")
		return
	}

	key := args[0]
	count, err := h.store.SCard(key)
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteInt(int(count))
	}
}

// 处理SMEMBERS命令
func (h *Handler) handleSMembers(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'smembers' command")
		return
	}

	key := args[0]
	members, err := h.store.SMembers(key)
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteArray(len(members))
		for _, member := range members {
			conn.WriteBulkString(member)
		}
	}
}

// 处理SINTER命令
func (h *Handler) handleSInter(conn redcon.Conn, args [][]byte) {
	if len(args) < 1 {
		conn.WriteError("ERR wrong number of arguments for 'sinter' command")
		return
	}

	keys := make([]string, len(args))
	for i, k := range args {
		keys[i] = string(k)
	}

	result, err := h.store.SInter(keys...)
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteArray(len(result))
		for _, member := range result {
			conn.WriteBulkString(member)
		}
	}
}

// 处理SDIFF命令
func (h *Handler) handleSDiff(conn redcon.Conn, args [][]byte) {
	if len(args) < 1 {
		conn.WriteError("ERR wrong number of arguments for 'sdiff' command")
		return
	}

	keys := make([]string, len(args))
	for i, k := range args {
		keys[i] = string(k)
	}

	result, err := h.store.SDiff(keys...)
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteArray(len(result))
		for _, member := range result {
			conn.WriteBulkString(member)
		}
	}
}

// 处理SUNION命令
func (h *Handler) handleSUnion(conn redcon.Conn, args [][]byte) {
	if len(args) < 1 {
		conn.WriteError("ERR wrong number of arguments for 'sunion' command")
		return
	}

	keys := make([]string, len(args))
	for i, k := range args {
		keys[i] = string(k)
	}

	result, err := h.store.SUnion(keys...)
	switch {
	case errors.Is(err, bitcaskredis.ErrWrongTypeOperation):
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
	case err != nil:
		conn.WriteError("ERR " + err.Error())
	default:
		conn.WriteArray(len(result))
		for _, member := range result {
			conn.WriteBulkString(member)
		}
	}
}

// zset

func (h *Handler) handleZAdd(conn redcon.Conn, args [][]byte) {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for 'zadd' command")
		return
	}
	key := args[0]
	var added int64
	// 遍历 score-member 对
	for i := 1; i < len(args); i += 2 {
		scoreBytes := args[i]
		member := args[i+1]
		score, err := strconv.ParseFloat(string(scoreBytes), 64)
		if err != nil {
			conn.WriteError("ERR invalid float score: " + string(scoreBytes))
			return
		}
		ok, err := h.store.ZAdd(key, score, member)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if ok {
			added++
		}
	}
	conn.WriteInt64(added)
}

func (h *Handler) handleZRem(conn redcon.Conn, args [][]byte) {
	if len(args) < 2 {
		conn.WriteError("ERR wrong number of arguments for 'zrem' command")
		return
	}

	key := args[0]
	var removed int64

	// 遍历 members
	for _, member := range args[1:] {
		ok, err := h.store.ZRem(key, member)
		if err != nil {
			conn.WriteError("ERR " + err.Error())
			return
		}
		if ok {
			removed++
		}
	}
	conn.WriteInt64(removed)
}

func (h *Handler) handleZScore(conn redcon.Conn, args [][]byte) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'zscore' command")
		return
	}
	key := args[0]
	member := args[1]
	score, err := h.store.ZScore(key, member)
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			conn.WriteNull()
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}

	if math.IsInf(score, -1) {
		conn.WriteNull()
	} else {
		conn.WriteBulkString(strconv.FormatFloat(score, 'f', -1, 64))
	}
}

func (h *Handler) handleZCard(conn redcon.Conn, args [][]byte) {
	if len(args) != 1 {
		conn.WriteError("ERR wrong number of arguments for 'zcard' command")
		return
	}

	key := args[0]
	card, err := h.store.ZCard(key)
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			conn.WriteInt(0)
		} else {
			conn.WriteError("ERR " + err.Error())
		}
		return
	}
	conn.WriteInt(int(card))
}

func (h *Handler) handleZRange(conn redcon.Conn, args [][]byte, reverse bool) {
	// 参数验证
	if len(args) < 3 || len(args) > 4 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}

	key := args[0]
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		conn.WriteError("ERR invalid start index")
		return
	}

	end, err := strconv.Atoi(string(args[2]))
	if err != nil {
		conn.WriteError("ERR invalid end index")
		return
	}

	// 解析 WITHSCORES
	withScores := false
	if len(args) == 4 {
		if strings.ToUpper(string(args[3])) != "WITHSCORES" {
			conn.WriteError("ERR syntax error")
			return
		}
		withScores = true
	}

	// 执行查询
	results, err := h.store.ZRange(key, start, end, reverse, withScores)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	// 返回结果
	conn.WriteArray(len(results))
	for _, res := range results {
		conn.WriteBulkString(res)
	}
}

func (h *Handler) handleZRangeByScore(conn redcon.Conn, args [][]byte, reverse bool) {
	if len(args) < 3 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}

	key := args[0]
	min, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		conn.WriteError("ERR invalid min score")
		return
	}

	max, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		conn.WriteError("ERR invalid max score")
		return
	}

	// 解析额外参数
	withScores := false
	offset := 0
	count := 0
	for i := 3; i < len(args); {
		switch strings.ToUpper(string(args[i])) {
		case "WITHSCORES":
			withScores = true
			i++
		case "LIMIT":
			if i+3 > len(args) {
				conn.WriteError("ERR syntax error")
				return
			}
			offset, _ = strconv.Atoi(string(args[i+1]))
			count, _ = strconv.Atoi(string(args[i+2]))
			i += 3
		default:
			conn.WriteError("ERR syntax error")
			return
		}
	}

	// 执行查询
	results, err := h.store.ZRangeByScore(key, min, max, reverse, withScores, offset, count)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	// 返回结果
	conn.WriteArray(len(results))
	for _, res := range results {
		conn.WriteBulkString(res)
	}
}

func (h *Handler) handleZRank(conn redcon.Conn, args [][]byte, reverse bool) {
	if len(args) != 2 {
		conn.WriteError("ERR wrong number of arguments")
		return
	}

	key := args[0]
	member := args[1]

	rank, err := h.store.ZRank(key, member, reverse)
	if err != nil {
		conn.WriteError("ERR " + err.Error())
		return
	}

	if rank == -1 {
		conn.WriteNull()
	} else {
		conn.WriteInt64(int64(rank))
	}
}
