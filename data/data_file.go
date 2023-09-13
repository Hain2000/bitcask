package data

import (
	"bitcask/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

var (
	ErrInvalidCRC = errors.New("invalid crc, log record maybe corrupted")
)

const FileNameSuffix = ".data"

type File struct {
	FileId    uint32
	WriteOff  int64         // 文件写到哪个位置了
	IoManager fio.IOManager // io读写管理
}

// OpenDataFile 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*File, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+FileNameSuffix)
	nIOM, err := fio.NewIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &File{
		FileId:    fileId,
		WriteOff:  0,
		IoManager: nIOM,
	}, nil
}

func (df *File) Sync() error {
	return df.IoManager.Sync()
}

func (df *File) Close() error {
	return df.IoManager.Close()
}

func (df *File) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}
	df.WriteOff += int64(n)
	return nil
}

func (df *File) GetLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+maxLogRecordHeaderSize > fileSize {
		headerBytes = fileSize + offset
	}

	headerBuf, err := df.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}

	header, hs := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}
	ks, vs := int64(header.keySize), int64(header.valueSize)
	var logRecordSize = hs + ks + vs

	logRecord := &LogRecord{Type: header.recordType}

	if ks > 0 || vs > 0 {
		kvBuf, err := df.readNBytes(ks+vs, offset+hs)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:ks]
		logRecord.Value = kvBuf[vs:]
	}
	//
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:hs])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}
	return logRecord, logRecordSize, nil
}

func (df *File) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return b, err
}