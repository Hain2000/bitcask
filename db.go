package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type DB struct {
	options         Options
	mtx             *sync.RWMutex
	fileIds         []int                 // 文件id，只能在加载索引的时候使用，不能在其他的地方更新和使用
	activeFile      *data.File            // 当前活跃文件，用于写入
	oldFile         map[uint32]*data.File // 旧的数据文件，只能用于读
	index           index.Indexer         // 内存索引
	seqNo           uint64                // 事务序列号，全局递增
	isMerging       bool                  // 是否有Merge进行
	seqNoFileExists bool                  // 存储事务序列号是否存在
	isInitial       bool                  // 是否第一次初始化数据目录
}

const seqNoKey = "seq.no"

func Open(options Options) (*DB, error) {
	// 检查用户配置
	if err := checkOptions(options); err != nil {
		return nil, err
	}
	var isInitial bool
	// 判断目录是否存在，如果不存在就创建
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		// 使用os.ModePerm可以获取权限
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}

	}
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}
	// 初始化 DB 实例结构体
	db := &DB{
		options:   options,
		mtx:       new(sync.RWMutex),
		oldFile:   make(map[uint32]*data.File),
		index:     index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrite),
		isInitial: isInitial,
	}

	// 加载merge数据目录
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	// 加载数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	// BPlusTree索引 不需要从数据文件加载索引
	if options.IndexType != BPLUSTREE {
		// 从hint索引文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		if err := db.loadIndexFromDataFiles(); err != nil {
			return nil, err
		}
	}
	// 取出当前事务索引号
	if options.IndexType == BPLUSTREE {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}
	return db, nil
}

// loadDataFile 从磁盘加载数据文件
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中的所有文件，找到所有以data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.FileNameSuffix) {
			//
			splitNames := strings.Split(entry.Name(), ".")
			curId, err := strconv.Atoi(splitNames[0])
			// 数据目录又可能损坏了
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, curId)
		}
	}
	// 对fileIds从小到达排序
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//
	for i, fid := range fileIds {
		curFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { // 最后一个id是最大的，说明是当前的活跃文件
			db.activeFile = curFile
		} else { // 老文件
			db.oldFile[uint32(fid)] = curFile
		}
	}
	return nil
}

// loadIndexFromDataFiles 从数据文件加载索引，遍历文件所有记录，并更新到索引中
func (db *DB) loadIndexFromDataFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	// 查看是否发生过merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFileName := filepath.Join(db.options.DirPath, data.FinishedFileName)
	if _, err := os.Stat(mergeFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, ty data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if ty == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var curSqeNo = nonTransactionSqeNo

	for i, v := range db.fileIds {
		var curFid = uint32(v)
		// 这些已经在Hint文件中加载索引了
		if hasMerge && curFid < nonMergeFileId {
			continue
		}
		var dataFile *data.File
		if curFid == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.oldFile[curFid]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.GetLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// 构造内存索引并保存
			logRecordPos := &data.LogRecordPos{Fid: curFid, Offset: offset}

			// 解析key拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSqeNo {
				// 非事务操作，直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务完成，对应的seq no的数据可以更新到内存索引中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			if seqNo > curSqeNo {
				curSqeNo = seqNo
			}

			// 递增offset,下一次从新的位置开始读取
			offset += size
		}

		// 如果当前是活跃文件，更新这个文件 WriteOff
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	// 更新事务序列号
	db.seqNo = curSqeNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}

	if options.DataFileSize <= 0 {
		return errors.New("database data file must be greater than 0")
	}
	return nil
}

// Put 写入key/value数据，key不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSqeNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入到当前活跃数据文件中
	pos, err := db.appendLogRecordWithLock(logRecord)
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

	return db.getValueByPosition(logRecordPos)
}

// Delete 删除key对于的值
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	// 构造【已删除】元素
	nLogRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSqeNo),
		Type: data.LogRecordDeleted,
	}
	_, err := db.appendLogRecordWithLock(nLogRecord)
	if err != nil {
		return nil
	}

	delIndexOk := db.index.Delete(key)
	if !delIndexOk {
		return ErrIndexUpdateFailed
	}
	return nil
}

// appendLogRecordWithLock 追加写数据到活跃文件中
func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	return db.appendLogRecord(logRecord)
}

// appendLogRecord 追加写数据到活跃文件中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

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

// getValueByPosition 获取数据文件中的Value
func (db *DB) getValueByPosition(pos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.File
	if db.activeFile.FileId == pos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.oldFile[pos.Fid]
	}

	// 数据文件为空
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量找信息
	logRecord, _, err := dataFile.GetLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
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

// Fold 获取所有数据并非用户指定的数据
func (db *DB) Fold(f func(key []byte, value []byte) bool) error {
	db.mtx.RLock()
	defer db.mtx.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		if !f(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()
	// 关闭索引
	if err := db.index.Close(); err != nil {
		return err
	}
	// 保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	//
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)), //
	}
	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}

	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	if err := db.activeFile.Close(); err != nil {
		return err
	}

	for _, file := range db.oldFile {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mtx.Lock()
	defer db.mtx.Unlock()
	return db.activeFile.Sync()
}

func (db *DB) loadSeqNo() error {
	fileName := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record, _, err := seqNoFile.GetLogRecord(0)
	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}
	db.seqNo = seqNo
	db.seqNoFileExists = true
	return nil
}
