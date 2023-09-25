package redis

import (
	"encoding/binary"
	"math"
)

const (
	maxMataDataSize   = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	extraListMetaSize = binary.MaxVarintLen64 * 2
	initialListMark   = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte
	expire   int64  // 过期事件
	version  int64  // 版本号（时间）
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
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
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
	expire, n := binary.Varint(buf[index:])
	index += n
	version, n := binary.Varint(buf[index:])
	index += n
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
		expire:   expire,
		version:  version,
		size:     uint32(size),
		head:     head,
		tail:     tail,
	}
}

type hashInternalKey struct {
	key     []byte
	version int64
	filed   []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.filed)+8)
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hk.version))
	index += 8

	// filed
	copy(buf[index:], hk.filed)

	return buf
}
