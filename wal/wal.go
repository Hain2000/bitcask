package wal

import (
	"bitcask/utils"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	initialSegmentFileID = 1
)

var (
	ErrValueTooLarge       = errors.New("the data size can't larger than segment size")
	ErrPendingSizeTooLarge = errors.New("the upper bound of pendingWrites can't larger than segment size")
)

type WAL struct {
	activeSegment     *segment
	olderSegments     map[SegmentID]*segment
	options           Options
	mu                sync.RWMutex
	bytesWrite        uint32
	renameIds         []SegmentID
	pendingWrites     [][]byte
	pendingSize       int64
	pendingWritesLock sync.Mutex
	closeC            chan struct{}
	sycnTicker        *time.Ticker
}

type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

func Open(options Options) (*WAL, error) {
	if !strings.HasPrefix(options.SegmentFileExtension, ".") {
		return nil, utils.ErrorAt(fmt.Errorf("segment file extension must start with '.'"))
	}
	wal := &WAL{
		options:       options,
		olderSegments: make(map[SegmentID]*segment),
		pendingWrites: make([][]byte, 0),
		closeC:        make(chan struct{}),
	}
	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	// 先读出线段文件
	var segmentIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err := fmt.Sscanf(entry.Name(), "%d"+options.SegmentFileExtension, &id)
		if err != nil {
			continue
		}
		segmentIDs = append(segmentIDs, id)
	}

	if len(segmentIDs) == 0 {
		segment, err := openSegmentFile(options.DirPath, options.SegmentFileExtension, initialSegmentFileID)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
	} else {
		sort.Ints(segmentIDs)
		for i, segId := range segmentIDs {
			segment, err := openSegmentFile(options.DirPath, options.SegmentFileExtension, uint32(segId))
			if err != nil {
				return nil, err
			}
			if i == len(segmentIDs)-1 {
				wal.activeSegment = segment
			} else {
				wal.olderSegments[segment.id] = segment
			}
		}
	}

	if wal.options.SyncInterval > 0 {
		wal.sycnTicker = time.NewTicker(wal.options.SyncInterval)
		go func() {
			for {
				select {
				case <-wal.sycnTicker.C:
					_ = wal.Sync()
				case <-wal.closeC:
					wal.sycnTicker.Stop()
					return
				}
			}
		}()
	}
	return wal, nil
}

func (wal *WAL) OpenActiveSegment() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}

	// 创建一个新的线段文件，然后让新文件成为活跃文件
	segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExtension, wal.activeSegment.id+1)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = segment
	wal.activeSegment = segment
	return nil
}

func (wal *WAL) ActiveSegmentID() SegmentID {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return wal.activeSegment.id
}

func (wal *WAL) IsEmpty() bool {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return len(wal.olderSegments) == 0 && wal.activeSegment.Size() == 0
}

func (wal *WAL) SetIsStartupTraversal(v bool) {
	for _, seg := range wal.olderSegments {
		seg.isStartupTraversal = v
	}
	wal.activeSegment.isStartupTraversal = v
}

func (wal *WAL) NewReaderWithMax(segId SegmentID) *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	var segmentReaders []*segmentReader
	for _, segment := range wal.olderSegments {
		if segId == 0 || segment.id <= segId {
			reader := segment.NewReader()
			segmentReaders = append(segmentReaders, reader)
		}
	}
	if segId == 0 || wal.activeSegment.id <= segId {
		reader := wal.activeSegment.NewReader()
		segmentReaders = append(segmentReaders, reader)
	}

	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})
	return &Reader{segmentReaders: segmentReaders, currentReader: 0}
}

func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
}

func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}
	data, position, err := r.segmentReaders[r.currentReader].Next()
	if err == io.EOF {
		r.currentReader++
		return r.Next()
	}
	return data, position, err
}

func (r *Reader) SkipCurrentSegment() {
	r.currentReader++
}

func (r *Reader) CurrentSegmentId() SegmentID {
	return r.segmentReaders[r.currentReader].segment.id
}

func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	reader := r.segmentReaders[r.currentReader]
	return &ChunkPosition{
		SegmentID:   reader.segment.id,
		BlockNumber: reader.blockNumber,
		ChunkOffset: reader.chunkOffset,
	}
}

func (wal *WAL) ClearPendingWrites() {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()
	wal.pendingSize = 0
	wal.pendingWrites = wal.pendingWrites[:0]
}

func (wal *WAL) PendingWrites(data []byte) {
	wal.pendingWritesLock.Lock()
	defer wal.pendingWritesLock.Unlock()
	size := wal.maxDataWriteSize(int64(len(data)))
	wal.pendingSize += size
	wal.pendingWrites = append(wal.pendingWrites, data)
}

func (wal *WAL) rotateActiveSegment() error {
	// 创建一个新的段文件并替换activeSegment
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	wal.bytesWrite = 0
	segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExtension, wal.activeSegment.id+1)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

func (wal *WAL) WriteAll() ([]*ChunkPosition, error) {
	if len(wal.pendingWrites) == 0 {
		return make([]*ChunkPosition, 0), nil
	}
	wal.mu.Lock()
	defer func() {
		wal.ClearPendingWrites()
		wal.mu.Unlock()
	}()
	if wal.pendingSize > wal.options.SegmentSize {
		return nil, ErrPendingSizeTooLarge
	}

	// 活跃文件满了，就新创建一个
	if wal.activeSegment.Size()+wal.pendingSize > wal.options.SegmentSize {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	positions, err := wal.activeSegment.writeAll(wal.pendingWrites)
	if err != nil {
		return nil, err
	}
	return positions, err
}

func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if int64(len(data))+chunkHeaderSize > wal.options.SegmentSize {
		return nil, ErrValueTooLarge
	}

	if wal.IsFull(int64(len(data))) {
		if err := wal.rotateActiveSegment(); err != nil {
			return nil, err
		}
	}

	position, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	wal.bytesWrite += position.ChunkSize

	var needSync = wal.options.Sync
	if !needSync && wal.options.BytePerSync > 0 {
		needSync = wal.bytesWrite >= wal.options.BytePerSync
	}
	if needSync {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
	}
	return position, err
}

func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	var segment *segment
	if pos.SegmentID == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[pos.SegmentID]
	}
	if segment == nil {
		return nil, utils.ErrorAt(fmt.Errorf("segment file %d%s not found", pos.SegmentID, wal.options.SegmentFileExtension))
	}
	return segment.Read(pos.BlockNumber, pos.ChunkOffset)
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	select {
	case <-wal.closeC:
	default:
		close(wal.closeC)
	}
	for _, segment := range wal.olderSegments {
		if err := segment.Close(); err != nil {
			return err
		}
		wal.renameIds = append(wal.renameIds, segment.id)
	}
	wal.olderSegments = nil
	wal.renameIds = append(wal.renameIds, wal.activeSegment.id)
	return wal.activeSegment.Close()
}

func (wal *WAL) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	for _, segment := range wal.olderSegments {
		if err := segment.Remove(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil
	return wal.activeSegment.Remove()
}

func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return wal.activeSegment.Sync()
}

func (wal *WAL) RenameFileExt(ext string) error {
	if !strings.HasPrefix(ext, ".") {
		return fmt.Errorf("segment file extension must start with '.'")
	}
	wal.mu.Lock()
	defer wal.mu.Unlock()
	renameFile := func(id SegmentID) error {
		oldName := SegmentFileName(wal.options.DirPath, wal.options.SegmentFileExtension, id)
		newName := SegmentFileName(wal.options.DirPath, ext, id)
		return os.Rename(oldName, newName)
	}
	for _, id := range wal.renameIds {
		if err := renameFile(id); err != nil {
			return err
		}
	}

	wal.options.SegmentFileExtension = ext
	return nil
}

func (wal *WAL) IsFull(delta int64) bool {
	return wal.activeSegment.Size()+wal.maxDataWriteSize(delta) > wal.options.SegmentSize
}

func (wal *WAL) maxDataWriteSize(size int64) int64 {
	// the maximum size = max_padding + (num_block + 1) * headerSize + dataSize
	return chunkHeaderSize + size + (size/blockSize+1)*chunkHeaderSize
}
