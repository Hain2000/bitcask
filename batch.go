package bitcask

import (
	"bytes"
	"fmt"
	"github.com/Hain2000/bitcask/data"
	"github.com/Hain2000/bitcask/utils"
	"github.com/bwmarrin/snowflake"
	"github.com/valyala/bytebufferpool"
	"sync"
	"time"
)

type Batch struct {
	options          BatchOptions
	mtx              sync.RWMutex
	db               *DB
	pendingWrites    []*data.LogRecord
	pendingWritesMap map[uint64][]int // map[uint64][]int比用map[string][]int节省内存：1.uint64只占8字节 2.[]byte转string会有额外内存开销 3.查找遍历整个字符串并逐字节比较，速度慢
	committed        bool
	rollbacked       bool
	batchId          *snowflake.Node
	buffers          []*bytebufferpool.ByteBuffer
}

func (db *DB) NewBatch(opts BatchOptions) *Batch {
	batch := &Batch{
		options:    opts,
		db:         db,
		committed:  false,
		rollbacked: false,
	}
	if !opts.ReadOnly {
		node, err := snowflake.NewNode(1)
		if err != nil {
			panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
		}
		batch.batchId = node
	}
	batch.lock()
	return batch
}

func newBatch() interface{} {
	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(fmt.Sprintf("snowflake.NewNode(1) failed: %v", err))
	}
	return &Batch{
		options: DefaultBatchOptions,
		batchId: node,
	}
}

func newRecord() interface{} {
	return &data.LogRecord{}
}

func (b *Batch) init(rdonly, sync bool, db *DB) {
	b.options.ReadOnly = rdonly
	b.options.Sync = sync
	b.db = db
	b.lock()
}

func (b *Batch) lock() {
	if b.options.ReadOnly {
		b.db.mtx.RLock()
	} else {
		b.db.mtx.Lock()
	}
}

func (b *Batch) unlock() {
	if b.options.ReadOnly {
		b.db.mtx.RUnlock()
	} else {
		b.db.mtx.Unlock()
	}
}

func (b *Batch) reset() {
	b.db = nil
	b.pendingWritesMap = nil
	b.pendingWrites = b.pendingWrites[:0]
	b.committed = false
	b.rollbacked = false
	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	b.buffers = b.buffers[:0]
}

func (b *Batch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}

	b.mtx.Lock()
	defer b.mtx.Unlock()
	record := b.lookupPendingWrites(key)
	if record == nil {
		record = b.db.recordPool.Get().(*data.LogRecord)
		b.appendPendingWrites(key, record)
	}
	record.Key, record.Value = key, value
	record.Type, record.Expire = data.LogRecordNormal, 0
	return nil
}

func (b *Batch) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}
	b.mtx.Lock()
	record := b.lookupPendingWrites(key)
	if record == nil {
		record = b.db.recordPool.Get().(*data.LogRecord)
		b.appendPendingWrites(key, record)
	}
	record.Key, record.Value = key, value
	record.Type, record.Expire = data.LogRecordNormal, time.Now().Add(ttl).UnixNano()
	b.mtx.Unlock()
	return nil
}

func (b *Batch) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	if b.db.closed {
		return nil, ErrDBClosed
	}
	now := time.Now().UnixNano()
	b.mtx.RLock()
	record := b.lookupPendingWrites(key)
	b.mtx.RUnlock()
	if record != nil {
		if record.Type == data.LogRecordDeleted || record.IsExpired(now) {
			return nil, ErrKeyNotFound
		}
		return record.Value, nil
	}

	chunkPosition := b.db.index.Get(key)
	if chunkPosition == nil {
		return nil, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(chunkPosition)
	if err != nil {
		return nil, err
	}
	record = data.DecodeLogRecord(chunk)
	if record.Type == data.LogRecordDeleted {
		panic("Deleted data cannot exist in the index")
	}
	if record.IsExpired(now) {
		b.db.index.Delete(record.Key)
		return nil, ErrKeyNotFound
	}
	return record.Value, nil
}

func (b *Batch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}
	b.mtx.Lock()
	var exist bool
	record := b.lookupPendingWrites(key)
	if record != nil {
		record.Type = data.LogRecordDeleted
		record.Value = nil
		record.Expire = 0
		exist = true
	}
	if !exist {
		record = &data.LogRecord{
			Key:  key,
			Type: data.LogRecordDeleted,
		}
		b.appendPendingWrites(key, record)
	}
	b.mtx.Unlock()
	return nil
}

func (b *Batch) Exist(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, ErrKeyIsEmpty
	}
	if b.db.closed {
		return false, ErrDBClosed
	}
	now := time.Now().UnixNano()
	b.mtx.RLock()
	record := b.lookupPendingWrites(key)
	b.mtx.RUnlock()
	if record != nil {
		return record.Type != data.LogRecordDeleted && !record.IsExpired(now), nil
	}
	position := b.db.index.Get(key)
	if position == nil {
		return false, nil
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return false, err
	}
	record = data.DecodeLogRecord(chunk)
	if record.Type == data.LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(key)
		return false, nil
	}
	return true, nil
}

// Expire 重新设置key的ttl
func (b *Batch) Expire(key []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}
	b.mtx.Lock()
	defer b.mtx.Unlock()
	record := b.lookupPendingWrites(key)
	if record != nil {
		if record.Type == data.LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = time.Now().Add(ttl).UnixNano()
		return nil
	}
	position := b.db.index.Get(key)
	if position == nil {
		return ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return err
	}
	now := time.Now()
	record = data.DecodeLogRecord(chunk)
	if record.Type == data.LogRecordDeleted || record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return ErrKeyNotFound
	}
	record.Expire = now.Add(ttl).UnixNano()
	b.appendPendingWrites(key, record)
	return nil
}

// TTL 拿到key的TTL
func (b *Batch) TTL(key []byte) (time.Duration, error) {
	if len(key) == 0 {
		return -1, ErrKeyIsEmpty
	}
	if b.db.closed {
		return -1, ErrDBClosed
	}
	now := time.Now()
	b.mtx.Lock()
	defer b.mtx.Unlock()
	record := b.lookupPendingWrites(key)
	if record != nil {
		if record.Expire == 0 {
			return -1, nil
		}
		if record.Type == data.LogRecordDeleted || record.IsExpired(now.UnixNano()) {
			return -1, ErrKeyNotFound
		}
		return time.Duration(record.Expire - now.UnixNano()), nil
	}

	position := b.db.index.Get(key)
	if position == nil {
		return -1, ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return -1, err
	}
	record = data.DecodeLogRecord(chunk)
	if record.Type == data.LogRecordDeleted {
		return -1, ErrKeyNotFound
	}
	if record.IsExpired(now.UnixNano()) {
		b.db.index.Delete(key)
		return -1, ErrKeyNotFound
	}
	if record.Expire > 0 {
		return time.Duration(record.Expire - now.UnixNano()), nil
	}
	return -1, nil
}

// Persist 用来去除TTL
func (b *Batch) Persist(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly {
		return ErrReadOnlyBatch
	}
	b.mtx.Lock()
	defer b.mtx.Unlock()
	record := b.lookupPendingWrites(key)
	if record != nil {
		if record.Type == data.LogRecordDeleted || record.IsExpired(time.Now().UnixNano()) {
			return ErrKeyNotFound
		}
		record.Expire = 0
		return nil
	}
	position := b.db.index.Get(key)
	if position == nil {
		return ErrKeyNotFound
	}
	chunk, err := b.db.dataFiles.Read(position)
	if err != nil {
		return err
	}
	record = data.DecodeLogRecord(chunk)
	now := time.Now().UnixNano()
	if record.Type == data.LogRecordDeleted || record.IsExpired(now) {
		b.db.index.Delete(key)
		return ErrKeyNotFound
	}
	if record.Expire == 0 {
		return nil
	}
	record.Expire = 0
	b.appendPendingWrites(key, record)
	return nil
}

// Commit 提交事务，暂存的数据
func (b *Batch) Commit() error {
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}
	if b.options.ReadOnly || len(b.pendingWrites) == 0 {
		return nil
	}
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	batchId := b.batchId.Generate()
	now := time.Now().UnixNano()
	for _, record := range b.pendingWrites {
		buf := bytebufferpool.Get()
		b.buffers = append(b.buffers, buf)
		record.BatchId = uint64(batchId)
		encRecord := data.EncodeLogRecord(record, b.db.encodeHeader, buf)
		b.db.dataFiles.PendingWrites(encRecord)
	}

	buf := bytebufferpool.Get()
	b.buffers = append(b.buffers, buf)
	encRecord := data.EncodeLogRecord(&data.LogRecord{
		Key:  batchId.Bytes(),
		Type: data.LogRecordBatchFinished,
	}, b.db.encodeHeader, buf)
	b.db.dataFiles.PendingWrites(encRecord)

	chunkPositions, err := b.db.dataFiles.WriteAll()
	if err != nil {
		b.db.dataFiles.ClearPendingWrites()
		return err
	}
	// 写入的数据(chunk) + BatchFinished
	if len(chunkPositions) != len(b.pendingWrites)+1 {
		panic("chunk positions length is not equal to pending writes length")
	}
	if b.options.Sync && !b.db.options.Sync {
		if err := b.db.dataFiles.Sync(); err != nil {
			return err
		}
	}
	for i, record := range b.pendingWrites {
		if record.Type == data.LogRecordDeleted || record.IsExpired(now) {
			b.db.index.Delete(record.Key)
		} else {
			b.db.index.Put(record.Key, chunkPositions[i])
		}
		b.db.recordPool.Put(record)
	}
	b.committed = true
	return nil
}

func (b *Batch) Rollback() error {
	defer b.unlock()
	if b.db.closed {
		return ErrDBClosed
	}

	if b.committed {
		return ErrBatchCommitted
	}
	if b.rollbacked {
		return ErrBatchRollbacked
	}

	for _, buf := range b.buffers {
		bytebufferpool.Put(buf)
	}
	if !b.options.ReadOnly {
		for _, record := range b.pendingWrites {
			b.db.recordPool.Put(record)
		}
		b.pendingWrites = b.pendingWrites[:0]
		for key := range b.pendingWritesMap {
			delete(b.pendingWritesMap, key)
		}
	}
	b.rollbacked = true
	return nil
}

func (b *Batch) lookupPendingWrites(key []byte) *data.LogRecord {
	if len(b.pendingWritesMap) == 0 {
		return nil
	}
	// key -> uint64
	hashKey := utils.MemHash(key)
	for _, entry := range b.pendingWritesMap[hashKey] {
		if bytes.Equal(b.pendingWrites[entry].Key, key) {
			return b.pendingWrites[entry]
		}
	}
	return nil
}

func (b *Batch) appendPendingWrites(key []byte, record *data.LogRecord) {
	b.pendingWrites = append(b.pendingWrites, record)
	if b.pendingWritesMap == nil {
		b.pendingWritesMap = make(map[uint64][]int)
	}
	hashKey := utils.MemHash(key)
	b.pendingWritesMap[hashKey] = append(b.pendingWritesMap[hashKey], len(b.pendingWrites)-1)
}
