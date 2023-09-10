package data

import (
	"bitcask/fio"
)

type DataFile struct {
	FileId    uint32
	WriteOff  int64         // 文件写到哪个位置了
	IoManager fio.IOManager // io读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (df *DataFile) Sync() error {
	return nil
}

func (df *DataFile) Write(buf []byte) error {
	return nil
}

func (df *DataFile) GetLogRecord(offset int64) (*LogRecord, error) {
	return nil, nil
}
