package bitcask

import (
	"bitcask/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "-merge"
	mergeFinishedKey = "merge-finished"
)

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}

	db.mtx.Lock()
	// 如果merge正在进行，则直接返回
	if db.isMerging {
		db.mtx.Unlock()
		return ErrMerageIsProgress
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	// 持久化活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mtx.Unlock()
		return err
	}

	// 当前文件转化为旧文件，并打开新文件
	db.oldFile[db.activeFile.FileId] = db.activeFile

	if err := db.setActiveDataFile(); err != nil {
		db.mtx.Unlock()
		return nil
	}

	noMergeFileId := db.activeFile.FileId

	var mergeFiles []*data.File
	for _, file := range db.oldFile {
		mergeFiles = append(mergeFiles, file)
	}
	db.mtx.Unlock()

	// 将merge的文件从小到大排序依次排序
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()
	// 如果有目录说明之前发生过merge，要删掉
	if _, err := os.Stat(mergePath); err != nil {
		if err = os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	// 新建一个 merge path 目录
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	mergeOpts := db.options
	mergeOpts.DirPath = mergePath
	mergeOpts.SyncWrite = false // 最后的时候在Sync
	mergeDB, err := Open(mergeOpts)
	if err != nil {
		return err
	}

	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	// 遍历处理每个数据文件
	for _, file := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := file.GetLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)

			// 和索引的比较，如果有效就重写
			if logRecordPos != nil && logRecordPos.Fid == file.FileId && logRecordPos.Offset == offset {
				// 消除事务标记
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSqeNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 当前位置写到Hint文件中
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}
			// 增加offset
			offset += size
		}
	}
	// 别忘了持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	// 写标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}

	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(noMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)

	if err := mergeFinishedFile.Write(encRecord); err != nil {
		return err
	}
	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath)) // path.Clean 可以删除多余路径分隔符“/”
	base := path.Base(db.options.DirPath)           // 拿到路径后一个元素
	return filepath.Join(dir, base+mergeDirName)    // 元素之间添加路径分隔符“/”
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		os.RemoveAll(mergePath)
	}()

	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}

	// 查找标识merge完成的文件，判断merge是否完成
	var isFinished bool = false
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.FinishedFileName {
			isFinished = true
		}

		if entry.Name() == data.SeqNoFileName {
			continue
		}
		
		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	if !isFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return nil
	}

	// 删掉旧数据文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		name := data.GetDataFileName(mergePath, fileId)
		if _, err := os.Stat(name); err != nil {
			if err := os.Remove(name); err != nil {
				return err
			}
		}
	}

	//
	for _, name := range mergeFileNames {
		//
		srcPath := filepath.Join(mergePath, name)
		dstPath := filepath.Join(db.options.DirPath, name)
		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinishedFile.GetLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return nil
	}

	hitFile, err := data.OpenHintFile(hintFileName)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		logRecord, size, err := hitFile.GetLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 要解码
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
