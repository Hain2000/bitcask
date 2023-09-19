package bitcask

import (
	"bitcask/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSqeNo uint64 = 0

var txnFinKey = []byte("txn-fin")

type WriteBatch struct {
	options       WriteBatchOptions
	mtx           *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

func (db *DB) NewWriteBatch(opts WriteBatchOptions) *WriteBatch {
	if db.options.IndexType == BPLUSTREE && !db.seqNoFileExists && !db.isInitial {
		panic("cannot use write batch, seq no file not exists")
	}
	return &WriteBatch{
		options:       opts,
		mtx:           new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mtx.Lock()
	defer wb.mtx.Unlock()
	logRecord := &data.LogRecord{Key: key, Value: value}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}
	wb.mtx.Lock()
	defer wb.mtx.Unlock()

	// 数据不存在，则直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 暂存logRecord
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit 提交事务，暂存的数据
func (wb *WriteBatch) Commit() error {
	wb.mtx.Lock()
	defer wb.mtx.Unlock()
	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchNum {
		return ErrExceedMaxBatchNum
	}

	wb.db.mtx.Lock()
	defer wb.db.mtx.Unlock()
	// 当前事务序列号
	curSeqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// 先讲数据暂存起来
	tmpPositions := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeq(record.Key, curSeqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		tmpPositions[string(record.Key)] = logRecordPos
	}

	// 标识事务完成
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(txnFinKey, curSeqNo),
		Type: data.LogRecordTxnFinished,
	}

	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	// 根据配置是否决定持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 更新内存索引
	for _, record := range wb.pendingWrites {
		pos := tmpPositions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		}
		if record.Type == data.LogRecordDeleted {
			wb.db.index.Delete(record.Key)
		}
	}

	// 清空暂存数据
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// logRecordKeyWithSeq key + Seq Number 编码
func logRecordKeyWithSeq(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)

	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)
	return encKey
}

// parseLogRecordKey 解析LogRecord的key，获得实际的key和序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
