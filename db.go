package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"bitcask/utils"
	"bitcask/wal"
	"errors"
	"fmt"
	"github.com/bwmarrin/snowflake"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type DB struct {
	options       Options
	mtx           sync.RWMutex
	dataFiles     *wal.WAL
	hintFile      *wal.WAL
	index         index.Indexer // 内存索引
	mergerRunning uint32        // 是否有Merge进行
	fileLock      *flock.Flock  // 文件锁，多进程之间互斥
	closed        bool
	batchPool     sync.Pool
	recordPool    sync.Pool
	encodeHeader  []byte
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
	if err = db.loadIndex(); err != nil {
		return nil, err
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
	indexRecords := make(map[uint64][]*data.IndexRecord)
	reader := db.dataFiles.NewReader()
	db.dataFiles.SetIsStartupTraversal(true)
	for {
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
		} else {
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

	return nil
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

// CurListKeys 获取所有的key
func (db *DB) CurListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
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
