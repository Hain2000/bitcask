package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"bitcask/utils"
	"bitcask/wal"
	"context"
	"errors"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"github.com/robfig/cron/v3"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type DB struct {
	options          Options
	mtx              sync.RWMutex
	dataFiles        *wal.WAL
	hintFile         *wal.WAL
	index            index.Indexer // 内存索引
	mergerRunning    uint32        // 是否有Merge进行
	fileLock         *flock.Flock  // 文件锁，多进程之间互斥
	closed           bool
	batchPool        sync.Pool
	recordPool       sync.Pool
	encodeHeader     []byte
	expiredCursorKey []byte
	cronScheduler    *cron.Cron
}

type Stat struct {
	KeyNum   int   // Key的总数量
	DiskSize int64 // 所占磁盘空间大小
}

const (
	fileLockName       = "FLOCK"
	dataFileNameSuffix = ".SEG"
	hintFileNameSuffix = ".HINT"
	mergeFinNameSuffix = ".MERGEFIN"
)

func Open(options Options) (*DB, error) {
	// 检查用户配置
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断目录是否存在，如果不存在就创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		// 使用os.ModePerm可以获取权限
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 文件锁, 放在多个进程使用一个目录
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	// 如果有merge文件，需要先加载merge文件
	if err = loadMergeFiles(options.DirPath); err != nil {
		return nil, err
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:      options,
		index:        index.NewIndexer(),
		fileLock:     fileLock,
		encodeHeader: make([]byte, data.MaxLogRecordHeaderSize),
		batchPool:    sync.Pool{New: newBatch},
		recordPool:   sync.Pool{New: newRecord},
	}

	// 加载数据文件
	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return nil, err
	}
	// 加载索引
	if err = db.loadIndex(); err != nil {
		return nil, err
	}

	if len(options.AutoMergeCronExpr) > 0 {
		db.cronScheduler = cron.New(
			cron.WithParser(cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)),
		)
		_, err = db.cronScheduler.AddFunc(options.AutoMergeCronExpr, func() {
			_ = db.Merge(true)
		})
		if err != nil {
			return nil, err
		}
		db.cronScheduler.Start()
	}

	return db, nil
}

func (db *DB) openWalFiles() (*wal.WAL, error) {
	walFiles, err := wal.Open(wal.Options{
		DirPath:              db.options.DirPath,
		SegmentSize:          db.options.SegmentSize,
		SegmentFileExtension: dataFileNameSuffix,
		Sync:                 db.options.Sync,
		BytePerSync:          db.options.BytePerSync,
	})
	if err != nil {
		return nil, err
	}
	return walFiles, nil
}

func (db *DB) loadIndex() error {
	if err := db.loadIndexFromHintFile(); err != nil {
		return err
	}
	if err := db.loadIndexFromWAL(); err != nil {
		return err
	}
	return nil
}

func (db *DB) loadIndexFromWAL() error {
	mergeFinSegmentId, err := getMergeFinSegmentId(db.options.DirPath)
	if err != nil {
		return err
	}
	indexRecords := make(map[uint64][]*data.IndexRecord)
	reader := db.dataFiles.NewReader()
	now := time.Now().UnixNano()
	db.dataFiles.SetIsStartupTraversal(true)
	for {
		// 这部分从hint文件中加载
		if reader.CurrentSegmentId() <= mergeFinSegmentId {
			reader.SkipCurrentSegment()
			continue
		}

		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		record := data.DecodeLogRecord(chunk)
		if record.Type == data.LogRecordBatchFinished {
			batchId, err := snowflake.ParseBytes(record.Key)
			if err != nil {
				return err
			}
			for _, idxRecord := range indexRecords[uint64(batchId)] {
				if idxRecord.RecordType == data.LogRecordNormal {
					db.index.Put(idxRecord.Key, idxRecord.Position)
				} else if idxRecord.RecordType == data.LogRecordDeleted {
					db.index.Delete(idxRecord.Key)
				}
			}
			delete(indexRecords, uint64(batchId))
		} else if record.Type == data.LogRecordNormal && record.BatchId == mergeFinishedBatchID {
			// 别忘了加载merge文件里的
			db.index.Put(record.Key, position)
		} else {
			if record.IsExpired(now) {
				db.index.Delete(record.Key)
				continue
			}

			indexRecords[record.BatchId] = append(indexRecords[record.BatchId],
				&data.IndexRecord{
					Key:        record.Key,
					RecordType: record.Type,
					Position:   position,
				})
		}
	}
	db.dataFiles.SetIsStartupTraversal(false)
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.SegmentSize <= 0 {
		return errors.New("database data file must be greater than 0")
	}

	if len(options.AutoMergeCronExpr) > 0 {
		if _, err := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor).Parse(options.AutoMergeCronExpr); err != nil {
			return fmt.Errorf("database auto merge cron expression is invalid, err: %s", err)
		}
	}
	return nil
}

func (db *DB) GetBatch(rdonly bool) *Batch {
	batch := db.batchPool.Get().(*Batch)
	batch.init(rdonly, false, db)
	return batch
}

func (db *DB) PutBatch(batch *Batch) {
	batch.reset()
	db.batchPool.Put(batch)
}

func (db *DB) Put(key []byte, value []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db)
	if err := batch.Put(key, value); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

func (db *DB) PutWithTTL(key []byte, value []byte, ttl time.Duration) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db)
	if err := batch.PutWithTTL(key, value, ttl); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

func (db *DB) Get(key []byte) ([]byte, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Get(key)
}

// Delete 删除key对于的值
func (db *DB) Delete(key []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db)
	if err := batch.Delete(key); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

func (db *DB) Exist(key []byte) (bool, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.Exist(key)
}

func (db *DB) Expire(key []byte, ttl time.Duration) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db)
	if err := batch.Expire(key, ttl); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

func (db *DB) TTL(key []byte) (time.Duration, error) {
	batch := db.batchPool.Get().(*Batch)
	batch.init(true, false, db)
	defer func() {
		_ = batch.Commit()
		batch.reset()
		db.batchPool.Put(batch)
	}()
	return batch.TTL(key)
}

func (db *DB) Persist(key []byte) error {
	batch := db.batchPool.Get().(*Batch)
	defer func() {
		batch.reset()
		db.batchPool.Put(batch)
	}()
	batch.init(false, false, db)
	if err := batch.Persist(key); err != nil {
		_ = batch.Rollback()
		return err
	}
	return batch.Commit()
}

// CurListKeys 获取所有的key
func (db *DB) CurListKeys() []string {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([]string, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = string(iterator.Key())
		idx++
	}
	return keys
}

func (db *DB) checkValue(chunk []byte) []byte {
	record := data.DecodeLogRecord(chunk)
	now := time.Now().UnixNano()
	if record.Type != data.LogRecordDeleted && !record.IsExpired(now) {
		return record.Value
	}
	return nil
}

func (db *DB) Descend(handleFn func(k []byte, v []byte) (bool, error)) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	db.index.Descend(func(key []byte, position *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(position)
		if err != nil {
			return false, err
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

func (db *DB) DescendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	db.index.DescendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

func (db *DB) DescendLessOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	db.index.DescendLessOrEqual(key, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

func (db *DB) DescendKeys(pattern []byte, filterExpried bool, handleFn func(k []byte) (bool, error)) error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}
	db.index.Descend(func(key []byte, position *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpried {
			chunk, err := db.dataFiles.Read(position)
			if err != nil {
				return false, nil
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

func (db *DB) DescendKeysRange(startKey, endKey, pattern []byte, filterExpried bool, handleFn func(k []byte) (bool, error)) error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}
	db.index.DescendRange(startKey, endKey, func(key []byte, position *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpried {
			chunk, err := db.dataFiles.Read(position)
			if err != nil {
				return false, nil
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

func (db *DB) Ascend(handleFn func(k []byte, v []byte) (bool, error)) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	db.index.Ascend(func(key []byte, position *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(position)
		if err != nil {
			return false, err
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

func (db *DB) AscendRange(startKey, endKey []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	db.index.AscendRange(startKey, endKey, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

func (db *DB) AscendGreaterOrEqual(key []byte, handleFn func(k []byte, v []byte) (bool, error)) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	db.index.AscendGreaterOrEqual(key, func(key []byte, pos *wal.ChunkPosition) (bool, error) {
		chunk, err := db.dataFiles.Read(pos)
		if err != nil {
			return false, nil
		}
		if value := db.checkValue(chunk); value != nil {
			return handleFn(key, value)
		}
		return true, nil
	})
}

func (db *DB) AscendKeys(pattern []byte, filterExpried bool, handleFn func(k []byte) (bool, error)) error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}
	db.index.Ascend(func(key []byte, position *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpried {
			chunk, err := db.dataFiles.Read(position)
			if err != nil {
				return false, nil
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

func (db *DB) AscendKeysRange(startKey, endKey, pattern []byte, filterExpried bool, handleFn func(k []byte) (bool, error)) error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	var reg *regexp.Regexp
	if len(pattern) > 0 {
		var err error
		reg, err = regexp.Compile(string(pattern))
		if err != nil {
			return err
		}
	}
	db.index.AscendRange(startKey, endKey, func(key []byte, position *wal.ChunkPosition) (bool, error) {
		if reg != nil && !reg.Match(key) {
			return true, nil
		}
		if filterExpried {
			chunk, err := db.dataFiles.Read(position)
			if err != nil {
				return false, nil
			}
			if value := db.checkValue(chunk); value == nil {
				return true, nil
			}
		}
		return handleFn(key)
	})
	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}

	if err := db.closeFiles(); err != nil {
		return err
	}

	if err := db.fileLock.Unlock(); err != nil {
		return err
	}

	if db.cronScheduler != nil {
		db.cronScheduler.Stop()
	}

	db.closed = true
	return nil
}

func (db *DB) closeFiles() error {
	if err := db.dataFiles.Close(); err != nil {
		return err
	}
	if db.hintFile != nil {
		if err := db.hintFile.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	return db.dataFiles.Sync()
}

// Stat 索引数，文件数，可回收字节，磁盘占用量
func (db *DB) Stat() *Stat {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	diskSize, err := utils.DirSize(db.options.DirPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get dir size : %v", err))
	}
	return &Stat{
		KeyNum:   db.index.Size(),
		DiskSize: diskSize,
	}
}

// Backup 备份数据库，将数据文件拷贝到新目录中
func (db *DB) Backup(dir string) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	return utils.CopyDir(db.options.DirPath, dir, []string{fileLockName})
}

func (db *DB) DeleteExpiredKeys(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	done := make(chan struct{}, 1)

	var innerErr error
	now := time.Now().UnixNano()

	go func(ctx context.Context) {
		db.mtx.Lock()
		defer db.mtx.Unlock()
		for {
			positions := make([]*wal.ChunkPosition, 0, 100)
			db.index.AscendGreaterOrEqual(db.expiredCursorKey, func(key []byte, position *wal.ChunkPosition) (bool, error) {
				positions = append(positions, position)
				if len(positions) >= 100 {
					return false, nil
				}
				return true, nil
			})

			if len(positions) == 0 {
				db.expiredCursorKey = nil
				done <- struct{}{}
				return
			}

			for _, position := range positions {
				chunk, err := db.dataFiles.Read(position)
				if err != nil {
					innerErr = err
					done <- struct{}{}
					return
				}
				record := data.DecodeLogRecord(chunk)
				if record.IsExpired(now) {
					db.index.Delete(record.Key)
				}
				db.expiredCursorKey = record.Key
			}
		}
	}(ctx)

	select {
	case <-ctx.Done():
		return innerErr
	case <-done:
		return innerErr
	}
}
