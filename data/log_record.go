package data

import (
	"encoding/binary"
	"github.com/Hain2000/bitcask/wal"
	"github.com/valyala/bytebufferpool"
)

type LogRecordType = byte

// maxLogRecordHeaderSize  (type)1 + (batchId)10 + (expire)10 + (k size)5 + (v size)5
const MaxLogRecordHeaderSize = 1 + binary.MaxVarintLen32*2 + binary.MaxVarintLen64*2

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordBatchFinished
)

type LogRecord struct {
	Key     []byte
	Value   []byte
	Type    LogRecordType
	BatchId uint64
	Expire  int64
}

type IndexRecord struct {
	Key        []byte
	RecordType LogRecordType
	Position   *wal.ChunkPosition
}

// encodeLogRecord 转换LogRecord为字符数组和长度
func EncodeLogRecord(logRecord *LogRecord, header []byte, buf *bytebufferpool.ByteBuffer) []byte {
	// { type | batch id | key size | value size | expire | key | value }
	header[0] = logRecord.Type
	var index = 1
	index += binary.PutUvarint(header[index:], logRecord.BatchId)
	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))
	index += binary.PutVarint(header[index:], logRecord.Expire)
	_, _ = buf.Write(header[:index])
	_, _ = buf.Write(logRecord.Key)
	_, _ = buf.Write(logRecord.Value)
	return buf.Bytes()
}

// decodeLogRecordHeader {头部， 头长}
func DecodeLogRecord(buf []byte) *LogRecord {
	recordType := buf[0]
	var index uint32 = 1
	batchId, n := binary.Uvarint(buf[index:])
	index += uint32(n)
	keySize, n := binary.Varint(buf[index:])
	index += uint32(n)
	valueSize, n := binary.Varint(buf[index:])
	index += uint32(n)
	expire, n := binary.Varint(buf[index:])
	index += uint32(n)

	key := make([]byte, keySize)
	copy(key[:], buf[index:index+uint32(keySize)])
	index += uint32(keySize)

	value := make([]byte, valueSize)
	copy(value[:], buf[index:index+uint32(valueSize)])
	return &LogRecord{
		Key:     key,
		Value:   value,
		Type:    recordType,
		BatchId: batchId,
		Expire:  expire,
	}
}

func EncodeHintRecord(key []byte, pos *wal.ChunkPosition) []byte {
	// (SegmentID)5 + (BlockNumber)5 + (ChunkOffset)10 + (ChunkSize)5 = 25
	buf := make([]byte, 25)
	var idx = 0
	idx += binary.PutUvarint(buf[idx:], uint64(pos.SegmentID))
	idx += binary.PutUvarint(buf[idx:], uint64(pos.BlockNumber))
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkOffset))
	idx += binary.PutUvarint(buf[idx:], uint64(pos.ChunkSize))
	res := make([]byte, idx+len(key))
	copy(res, buf[:idx])
	copy(res[idx:], key)
	return res
}

func DecodeHintRecord(buf []byte) ([]byte, *wal.ChunkPosition) {
	var idx = 0
	segmentId, n := binary.Uvarint(buf[idx:])
	idx += n
	blockNumber, n := binary.Uvarint(buf[idx:])
	idx += n
	chunkOffset, n := binary.Uvarint(buf[idx:])
	idx += n
	chunkSize, n := binary.Uvarint(buf[idx:])
	idx += n

	key := buf[idx:]

	return key, &wal.ChunkPosition{
		SegmentID:   wal.SegmentID(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}

func EncodeMergeFinRecord(segmentId wal.SegmentID) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, segmentId)
	return buf
}

func (lr *LogRecord) IsExpired(now int64) bool {
	// 被使用 > 0
	return lr.Expire > 0 && lr.Expire <= now
}
