package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"sync"
)

type DB struct {
	options    Options
	mtx        *sync.RWMutex
	activeFile *data.DataFile            // 当前活跃文件，用于写入
	oldFile    map[uint32]*data.DataFile // 旧的数据文件，只能用于读
	index      index.Indexer             // 内存索引
}

// Put 写入key/value数据，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Get 根据key得到value
func (db *DB) Get(key []byte) ([]byte, error) {
	// 判断key的有效性
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存的数据结构中把对于的key取出来
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}
	// 根据fid找到对于的数据文件

	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFile[logRecordPos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, err := dataFile.GetLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// appendLogRecord 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	// 判断当前活跃数据文件是否存在，没有写入的时候是没有文件生成的
	// 如果为空则初始化文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 写入数据编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果如果写入的数据已到达活跃文件的阈值，则关闭活跃文件，并代开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		// 先持久化文件，保证已有的数据持久到磁盘中
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 当前活跃文件转化为旧的文件
		db.oldFile[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 根据用户配置是否需要持久化
	if db.options.SyncWrite {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{Fid: db.activeFile.FileId, Offset: writeOff}
	return pos, nil
}

// 设置活跃文件
// 访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}

	// 打开新的数据文件
	dateFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dateFile
	return nil
}
