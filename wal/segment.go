package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/Hain2000/bitcask/utils"
	"github.com/valyala/bytebufferpool"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type ChunkType = byte
type SegmentID = uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid CRC, the data may be corrupted")
)

const (
	chunkHeaderSize = 7 // 4(checksum) + 2(size) + 1(type)
	blockSize       = 32 * KB
	fileModePerm    = 0644
	maxLen          = binary.MaxVarintLen32*3 + binary.MaxVarintLen64
)

type segment struct {
	id                 SegmentID
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
	header             []byte
	startupBlock       *startupBlock
	isStartupTraversal bool
}

type startupBlock struct {
	blockNumber int64
	block       []byte
}

type segmentReader struct {
	segment     *segment
	blockNumber uint32
	chunkOffset int64
}

type ChunkPosition struct {
	SegmentID   SegmentID
	BlockNumber uint32 // 当前已经写完的块
	ChunkOffset int64
	ChunkSize   uint32
}

var blockPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, blockSize)
	},
}

func getBuffer() []byte {
	return blockPool.Get().([]byte)
}

func putBuffer(buf []byte) {
	blockPool.Put(buf)
}

func SegmentFileName(dirPath, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

// openSegmentFile
func openSegmentFile(dirPath, extName string, id SegmentID) (*segment, error) {
	fd, err := os.OpenFile(
		SegmentFileName(dirPath, extName, id),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}

	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, extName, err)
	}

	// ???
	// 一个chunk是一条记录
	return &segment{
		id:                 id,
		fd:                 fd,
		header:             make([]byte, chunkHeaderSize),
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
		startupBlock: &startupBlock{
			block:       make([]byte, blockSize),
			blockNumber: -1,
		},
		isStartupTraversal: false,
	}, nil
}

func (seg *segment) NewReader() *segmentReader {
	// ???
	return &segmentReader{
		segment:     seg,
		blockNumber: 0,
		chunkOffset: 0,
	}
}

// file op

func (seg *segment) Sync() error {
	if seg.closed {
		return nil
	}
	// 将打开的文件写入磁盘
	return seg.fd.Sync()
}

func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		if err := seg.fd.Close(); err != nil {
			return err
		}
	}
	return os.Remove(seg.fd.Name())
}

func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}
	seg.closed = true
	return seg.fd.Close()
}

func (seg *segment) Size() int64 {
	return int64(seg.currentBlockNumber*blockSize) + int64(seg.currentBlockSize)
}

// write

func (seg *segment) writeToBuffer(data []byte, chunkBuffer *bytebufferpool.ByteBuffer) (*ChunkPosition, error) {
	startBufferLen := chunkBuffer.Len()
	padding := uint32(0)
	if seg.closed {
		return nil, utils.ErrorAt(ErrClosed)
	}
	// 确保block有存放data的空间
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		if seg.currentBlockSize < blockSize {
			p := make([]byte, blockSize-seg.currentBlockSize)
			chunkBuffer.B = append(chunkBuffer.B, p...)
			padding += blockSize - seg.currentBlockSize

			seg.currentBlockSize = 0
			seg.currentBlockNumber++
		}
	}

	position := &ChunkPosition{
		SegmentID:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}

	dataSize := uint32(len(data))
	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		seg.appendChunkToBuffer(chunkBuffer, data, ChunkTypeFull)
		position.ChunkSize = dataSize + chunkHeaderSize
	} else {
		// 如果这个块装不下data
		var (
			dataIdx      uint32 = 0
			blockCount   uint32 = 0
			curBlockSize        = seg.currentBlockSize
		)
		for dataIdx < dataSize {
			maxChunkSize := blockSize - curBlockSize - chunkHeaderSize
			chunkSize := min(maxChunkSize, dataSize-dataIdx)
			chunkType := ChunkTypeMiddle
			if dataIdx == 0 {
				chunkType = ChunkTypeFirst
			} else if dataIdx+chunkSize == dataSize {
				chunkType = ChunkTypeLast
			}
			nextIdx := dataIdx + chunkSize
			seg.appendChunkToBuffer(chunkBuffer, data[dataIdx:nextIdx], chunkType)
			dataIdx = nextIdx
			blockCount++
			curBlockSize = (curBlockSize + chunkSize + chunkHeaderSize) % blockSize
		}
		position.ChunkSize = blockCount*chunkHeaderSize + dataSize
	}
	endBufferLen := chunkBuffer.Len()
	if position.ChunkSize+padding != uint32(endBufferLen-startBufferLen) {
		return nil, fmt.Errorf("wrong!!! the chunk size %d is not equal to the buffer len %d",
			position.ChunkSize+padding, endBufferLen-startBufferLen)
	}
	seg.currentBlockSize += position.ChunkSize
	if seg.currentBlockSize >= blockSize {
		seg.currentBlockNumber += seg.currentBlockSize / blockSize
		seg.currentBlockSize %= blockSize
	}
	return position, nil
}

func (seg *segment) appendChunkToBuffer(buf *bytebufferpool.ByteBuffer, data []byte, chunkType ChunkType) {
	binary.LittleEndian.PutUint16(seg.header[4:6], uint16(len(data))) // length
	seg.header[6] = chunkType
	sum := crc32.ChecksumIEEE(seg.header[4:])
	sum = crc32.Update(sum, crc32.IEEETable, data)
	binary.LittleEndian.PutUint32(seg.header[:4], sum)
	buf.B = append(buf.B, seg.header...)
	buf.B = append(buf.B, data...)
}

func (seg *segment) writeAll(data [][]byte) (positions []*ChunkPosition, err error) {
	if seg.closed {
		return nil, utils.ErrorAt(ErrClosed)
	}
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize

	// init chunk buffer
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	// 还原
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
			bytebufferpool.Put(chunkBuffer)
		}
	}()
	var pos *ChunkPosition
	positions = make([]*ChunkPosition, len(data))
	for i := 0; i < len(positions); i++ {
		pos, err = seg.writeToBuffer(data[i], chunkBuffer)
		if err != nil {
			return
		}
		positions[i] = pos
	}
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return
}

func (seg *segment) writeChunkBuffer(buf *bytebufferpool.ByteBuffer) error {
	if seg.currentBlockSize > blockSize {
		return errors.New("the current block size exceeds the maximum block size")
	}
	if _, err := seg.fd.Write(buf.Bytes()); err != nil {
		return err
	}
	seg.startupBlock.blockNumber = -1
	return nil
}

func (seg *segment) Write(data []byte) (pos *ChunkPosition, err error) {
	if seg.closed {
		return nil, utils.ErrorAt(ErrClosed)
	}
	originBlockNumber := seg.currentBlockNumber
	originBlockSize := seg.currentBlockSize

	// init chunk buffer
	chunkBuffer := bytebufferpool.Get()
	chunkBuffer.Reset()
	defer func() {
		if err != nil {
			seg.currentBlockNumber = originBlockNumber
			seg.currentBlockSize = originBlockSize
		}
		bytebufferpool.Put(chunkBuffer)
	}()
	pos, err = seg.writeToBuffer(data, chunkBuffer)
	if err != nil {
		return
	}
	if err = seg.writeChunkBuffer(chunkBuffer); err != nil {
		return
	}
	return
}

// Read

func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	res, _, err := seg.readInternal(blockNumber, chunkOffset)
	return res, err
}

// readInternal
// Input: blockNumber 要读取的块号, chunkOffset 块内的起始偏移量
// Output:读取到的数据内容, 下一个块的起始位置信息, error
func (seg *segment) readInternal(blockNumber uint32, chunkOffset int64) ([]byte, *ChunkPosition, error) {
	if seg.closed {
		return nil, nil, utils.ErrorAt(ErrClosed)
	}
	var (
		result    []byte                              // 存储最终读取的数据
		block     []byte                              // 当前读取的块数据
		segSize   = seg.Size()                        // segment的总大小
		nextChunk = &ChunkPosition{SegmentID: seg.id} // 下一个块的位置信息
	)

	// 根据是否处于启动遍历模式选择块来源
	if seg.isStartupTraversal {
		block = seg.startupBlock.block // 使用预缓存的启动块
	} else {
		block = getBuffer() // 从缓冲区池获取临时块
		if len(block) != blockSize {
			block = make([]byte, blockSize) // 若缓冲区大小不符，新建块
		}
		defer putBuffer(block) // 函数退出时释放缓冲区
	}

	for {
		size := int64(blockSize)                 // 默认读取整个块
		offset := int64(blockNumber) * blockSize // 当前已经写完的块 * blockSize
		if size+offset > segSize {
			size = segSize - offset // 处理最后一个不完整块
		}
		if chunkOffset >= size {
			return nil, nil, io.EOF
		}

		// 先把整块数据读入到block中
		if seg.isStartupTraversal {
			// 仅在缓存块不匹配或块未满时重新读取
			if seg.startupBlock.blockNumber != int64(blockNumber) || size != blockSize {
				_, err := seg.fd.ReadAt(block[0:size], offset)
				if err != nil {
					return nil, nil, err
				}
				seg.startupBlock.blockNumber = int64(blockNumber) // 更新缓存块号
			}
		} else {
			if _, err := seg.fd.ReadAt(block[0:size], offset); err != nil {
				return nil, nil, err
			}
		}

		header := block[chunkOffset : chunkOffset+chunkHeaderSize]
		length := binary.LittleEndian.Uint16(header[4:6])
		start := chunkOffset + chunkHeaderSize
		result = append(result, block[start:start+int64(length)]...)

		checksumEnd := chunkOffset + chunkHeaderSize + int64(length) // header + data
		checksum := crc32.ChecksumIEEE(block[chunkOffset+4 : checksumEnd])
		savedSum := binary.LittleEndian.Uint32(header[:4])
		if savedSum != checksum {
			return nil, nil, ErrInvalidCRC
		}

		chunkType := header[6]
		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			nextChunk.BlockNumber = blockNumber
			nextChunk.ChunkOffset = checksumEnd
			// 当前这个块中剩余的空间装不下下一个chunk的header，下一个chunk就在下一个块中
			if checksumEnd+chunkHeaderSize >= blockSize {
				nextChunk.BlockNumber++
				nextChunk.ChunkOffset = 0
			}
			break
		}
		blockNumber++
		chunkOffset = 0
	}
	return result, nextChunk, nil
}

// Next 取出当前chunk，并且使regReader指向下一个chunk
func (segReader *segmentReader) Next() ([]byte, *ChunkPosition, error) {
	if segReader.segment.closed {
		return nil, nil, utils.ErrorAt(ErrClosed)
	}

	chunkPosition := &ChunkPosition{
		SegmentID:   segReader.segment.id,
		BlockNumber: segReader.blockNumber,
		ChunkOffset: segReader.chunkOffset,
	}

	value, nextChunk, err := segReader.segment.readInternal(segReader.blockNumber, segReader.chunkOffset)
	if err != nil {
		return nil, nil, err
	}
	chunkPosition.ChunkSize = nextChunk.BlockNumber*blockSize + uint32(nextChunk.ChunkOffset) - (segReader.blockNumber*blockSize + uint32(segReader.chunkOffset))
	segReader.blockNumber = nextChunk.BlockNumber
	segReader.chunkOffset = nextChunk.ChunkOffset
	return value, chunkPosition, nil
}

// Encode & Decode

func (cp *ChunkPosition) Encode() []byte {
	return cp.encode(true)
}

func (cp *ChunkPosition) EncodeFixedSize() []byte {
	return cp.encode(false)
}

func (cp *ChunkPosition) encode(shrink bool) []byte {
	buf := make([]byte, maxLen)
	var index = 0
	index += binary.PutUvarint(buf[index:], uint64(cp.SegmentID))
	index += binary.PutUvarint(buf[index:], uint64(cp.BlockNumber))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkOffset))
	index += binary.PutUvarint(buf[index:], uint64(cp.ChunkSize))
	if shrink {
		return buf[:index]
	}
	return buf
}

func DecodeChunkPosition(buf []byte) *ChunkPosition {
	if len(buf) == 0 {
		return nil
	}
	var index = 0
	segmentId, n := binary.Uvarint(buf[index:])
	index += n
	blockNumber, n := binary.Uvarint(buf[index:])
	index += n
	chunkOffset, n := binary.Uvarint(buf[index:])
	index += n
	chunkSize, n := binary.Uvarint(buf[index:])
	index += n
	return &ChunkPosition{
		SegmentID:   uint32(segmentId),
		BlockNumber: uint32(blockNumber),
		ChunkOffset: int64(chunkOffset),
		ChunkSize:   uint32(chunkSize),
	}
}
