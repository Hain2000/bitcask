package redis

import (
	"bitcask/utils"
	"encoding/binary"
	"math"
)

const (
	maxMataDataSize   = 1 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte
	size     uint32 // key里的数据量
	head     uint64 // List 专用
	tail     uint64 // List 专用
}

func (md *metadata) encode() []byte {
	var size = maxMataDataSize
	if md.dataType == List {
		size += extraListMetaSize
	}
	buf := make([]byte, size)

	buf[0] = md.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetaData(buf []byte) *metadata {
	dataType := buf[0]

	var index = 1
	size, n := binary.Varint(buf[index:])
	index += n

	var head uint64 = 0
	var tail uint64 = 0
	if dataType == List {
		head, n = binary.Uvarint(buf[index:])
		index += n
		tail, _ = binary.Uvarint(buf[index:])
	}
	return &metadata{
		dataType: dataType,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type setInternalKey struct {
	key    []byte
	member []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+8+4)
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)

	// member
	copy(buf[index:index+len(sk.member)], sk.member)
	index += len(sk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(sk.member)))

	return buf
}

type listInternalKey struct {
	key   []byte
	index uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8+8)
	// key
	var index = 0
	copy(buf[index:index+len(lk.key)], lk.key)
	index += len(lk.key)

	// index
	binary.LittleEndian.PutUint64(buf[index:], lk.index)

	return buf
}

type zsetInternalKey struct {
	key    []byte
	member []byte
	score  float64
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+len(scoreBuf)+8+4)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))

	return buf
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member)+8)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// member
	copy(buf[index:], zk.member)

	return buf
}
