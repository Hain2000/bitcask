package utils

import (
	"encoding/binary"
	"math"
	"strconv"
)

func Float64ToBytes(val float64) []byte {
	return []byte(strconv.FormatFloat(val, 'f', -1, 64))
}

func FloatFromBytes(val []byte) float64 {
	f, _ := strconv.ParseFloat(string(val), 64)
	return f
}

func EncodeFloat64ForBTree(value float64) []byte {
	bits := math.Float64bits(value)
	// 负数需要调整顺序，使其符合字节序
	if value < 0 {
		bits = ^bits // 按位取反，保证负数比正数小
	} else {
		bits |= 1 << 63 // 最高位设为 1，使正数比负数大
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, bits) // 使用大端序存储
	return buf
}

func DecodeFloat64FromBTree(encoded []byte) float64 {
	bits := binary.BigEndian.Uint64(encoded)
	if bits&(1<<63) != 0 { // 最高位是 1，说明是正数
		bits &= ^(uint64(1) << 63) // 还原原始 IEEE 754 格式
	} else { // 负数
		bits = ^bits // 取反恢复原始值
	}
	return math.Float64frombits(bits)
}

func DecodeFloat64FromBTreeToBytes(encoded []byte) []byte {
	return Float64ToBytes(DecodeFloat64FromBTree(encoded))
}
