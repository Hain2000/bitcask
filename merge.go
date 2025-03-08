package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"bitcask/wal"
	"encoding/binary"
	"fmt"
	"github.com/valyala/bytebufferpool"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

const (
	mergeDirSuffixName   = "-merge"
	mergeFinishedBatchID = 0
)

func (db *DB) Merge(reopenAfterDone bool) error {
	if err := db.doMerge(); err != nil {
		return err
	}
	if !reopenAfterDone {
		return nil
	}

	db.mtx.Lock()
	defer db.mtx.Unlock()
	_ = db.closeFiles()

	err := loadMergeFiles(db.options.DirPath)
	if err != nil {
		return err
	}

	if db.dataFiles, err = db.openWalFiles(); err != nil {
		return err
	}

	db.index = index.NewIndexer()
	if err = db.loadIndex(); err != nil {
		return err
	}

	return nil
}

func (db *DB) doMerge() error {
	db.mtx.Lock()
	if db.closed {
		db.mtx.Unlock()
		return ErrDBClosed
	}
	if db.dataFiles.IsEmpty() {
		db.mtx.Unlock()
		return nil
	}

	if atomic.LoadUint32(&db.mergerRunning) == 1 {
		db.mtx.Unlock()
		return ErrMerageIsProgress
	}

	atomic.StoreUint32(&db.mergerRunning, 1)
	defer atomic.StoreUint32(&db.mergerRunning, 0)

	preActiveSegId := db.dataFiles.ActiveSegmentID()
	if err := db.dataFiles.OpenActiveSegment(); err != nil {
		db.mtx.Unlock()
		return err
	}
	db.mtx.Unlock()

	mergeDB, err := db.openMergeDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = mergeDB.Close()
	}()

	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	now := time.Now().UnixNano()
	reader := db.dataFiles.NewReaderWithMax(preActiveSegId)
	for {
		buf.Reset()
		chunk, position, err := reader.Next()
		if err != nil {
			if err == io.EOF { // 读完了
				break
			}
			return err
		}
		record := data.DecodeLogRecord(chunk)
		if record.Type == data.LogRecordNormal && (record.Expire == 0 || record.Expire > now) {
			db.mtx.RLock()
			indexPos := db.index.Get(record.Key)
			db.mtx.RUnlock()
			// 只有normal log record, 也就是index里的数据, 读出来写入mergeDB中
			if indexPos != nil && positionEquals(indexPos, position) {
				record.BatchId = mergeFinishedBatchID
				newPosition, err := mergeDB.dataFiles.Write(data.EncodeLogRecord(record, mergeDB.encodeHeader, buf))
				if err != nil {
					return err
				}

				_, err = mergeDB.hintFile.Write(data.EncodeHintRecord(record.Key, newPosition))
				if err != nil {
					return err
				}
			}
		}
	}
	// 存下活跃segment id，后续合并用
	mergeFinFile, err := db.openMergeFinishedFile()
	if err != nil {
		return err
	}
	_, err = mergeFinFile.Write(data.EncodeMergeFinRecord(preActiveSegId))
	if err != nil {
		return err
	}

	if err := mergeFinFile.Close(); err != nil {
		return err
	}

	return nil
}

func (db *DB) openMergeDB() (*DB, error) {
	mergePath := mergeDirPath(db.options.DirPath)
	if err := os.RemoveAll(mergePath); err != nil {
		return nil, err
	}
	options := db.options
	options.Sync, options.BytePerSync = false, 0
	options.DirPath = mergePath
	mergeDB, err := Open(options)
	if err != nil {
		return nil, err
	}
	hintFile, err := wal.Open(wal.Options{
		DirPath:              options.DirPath,
		SegmentSize:          math.MaxInt64,
		SegmentFileExtension: hintFileNameSuffix,
		Sync:                 false,
		BytePerSync:          0,
	})
	if err != nil {
		return nil, err
	}
	mergeDB.hintFile = hintFile
	return mergeDB, nil
}

func (db *DB) openMergeFinishedFile() (*wal.WAL, error) {
	return wal.Open(wal.Options{
		DirPath:              db.options.DirPath,
		SegmentSize:          GB,
		SegmentFileExtension: mergeFinNameSuffix,
		Sync:                 false,
		BytePerSync:          0,
	})
}

func mergeDirPath(dirPath string) string {
	dir := filepath.Dir(filepath.Clean(dirPath))
	base := filepath.Base(dirPath)
	return filepath.Join(dir, base+mergeDirSuffixName)
}

func positionEquals(a, b *wal.ChunkPosition) bool {
	return a.SegmentID == b.SegmentID &&
		a.BlockNumber == b.BlockNumber &&
		a.ChunkOffset == b.ChunkOffset
}

func loadMergeFiles(dirPath string) error {
	mergeDirPath := mergeDirPath(dirPath)
	if _, err := os.Stat(mergeDirPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	defer func() {
		_ = os.RemoveAll(mergeDirPath)
	}()

	copyFile := func(suffix string, fileId uint32, force bool) {
		srcFile := wal.SegmentFileName(mergeDirPath, suffix, fileId)
		stat, err := os.Stat(srcFile)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			panic(fmt.Sprintf("loadMergeFiles: failed to get src file stat %v", err))
		}
		if !force && stat.Size() == 0 {
			return
		}
		dstFile := wal.SegmentFileName(dirPath, suffix, fileId)
		_ = os.Rename(srcFile, dstFile)
	}

	mergeFinSegmentId, err := getMergeFinSegmentId(mergeDirPath)
	if err != nil {
		return err
	}

	for fileId := uint32(1); fileId <= mergeFinSegmentId; fileId++ {
		dstFile := wal.SegmentFileName(dirPath, dataFileNameSuffix, fileId)

		if _, err := os.Stat(dstFile); err == nil {
			if err = os.Remove(dstFile); err != nil {
				return err
			}
		}
		copyFile(dataFileNameSuffix, fileId, false)
	}
	copyFile(mergeFinNameSuffix, 1, true)
	copyFile(hintFileNameSuffix, 1, true)
	return nil
}

func getMergeFinSegmentId(mergePath string) (wal.SegmentID, error) {
	mergeFinFile, err := os.Open(wal.SegmentFileName(mergePath, mergeFinNameSuffix, 1))
	if err != nil {
		return 0, nil
	}
	defer func() {
		_ = mergeFinFile.Close()
	}()
	// 拿出活跃文件segment id
	mergeFinBuf := make([]byte, 4)
	if _, err := mergeFinFile.ReadAt(mergeFinBuf, 7); err != nil {
		return 0, err
	}
	mergeFinSegmentId := binary.LittleEndian.Uint32(mergeFinBuf)
	return mergeFinSegmentId, nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFile, err := wal.Open(wal.Options{
		DirPath:              db.options.DirPath,
		SegmentSize:          math.MaxInt64,
		SegmentFileExtension: hintFileNameSuffix,
	})
	if err != nil {
		return err
	}
	defer func() {
		_ = hintFile.Close()
	}()

	reader := hintFile.NewReader()
	hintFile.SetIsStartupTraversal(true)
	for {
		chunk, _, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key, position := data.DecodeHintRecord(chunk)
		db.index.Put(key, position)
	}
	hintFile.SetIsStartupTraversal(false)
	return nil
}
