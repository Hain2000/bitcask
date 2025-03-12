package redis

import (
	"bitcask"
	"bitcask/utils"
	"encoding/binary"
	"errors"
)

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

// ZSet -----------------------------------------------------------

func (rds *DataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		return false, err
	}

	zk := &zsetInternalKey{
		key:    key,
		score:  score,
		member: member,
	}
	var exist = true
	// 查看是否已经存在了
	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		if score == utils.FloatFromBytes(value) {
			return false, nil
		}
	}

	// 更新元数据和数据
	wb := rds.db.NewBatch(bitcask.DefaultBatchOptions)
	if !exist {
		meta.size++
		wb.Put(key, meta.encode())
	}

	if exist {
		oldKey := &zsetInternalKey{
			key:    key,
			member: member,
			score:  utils.FloatFromBytes(value),
		}
		wb.Delete(oldKey.encodeWithScore())
	}
	wb.Put(zk.encodeWithMember(), utils.Float64ToBytes(score))
	wb.Put(zk.encodeWithScore(), nil)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *DataStructure) ZScore(key []byte, member []byte) (float64, error) {
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil { // 暂时不支持
		return -1, err
	}

	if meta.size == 0 {
		return -1, nil
	}

	zk := &zsetInternalKey{
		key:    key,
		member: member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}
