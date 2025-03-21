package redis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/utils"
	"math"
)

type zsetInternalKey struct {
	key    []byte
	member []byte
	score  float64
}

func (zk *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.EncodeFloat64ForBTree(zk.score)
	buf := make([]byte, len(zk.key)+len(zk.member)+8+4)

	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)

	// score
	copy(buf[index:index+8], scoreBuf)
	index += 8

	// member
	copy(buf[index:index+len(zk.member)], zk.member)
	index += len(zk.member)

	// member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zk.member)))
	return buf
}

func (zk *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zk.key)+len(zk.member))
	// key
	var index = 0
	copy(buf[index:index+len(zk.key)], zk.key)
	index += len(zk.key)
	// member
	copy(buf[index:], zk.member)
	return buf
}

func decodeZSetInternalKey(key, internalKey []byte) ([]byte, []byte) {
	scoreBytes := internalKey[len(key) : len(key)+8]
	memberSizeBytes := internalKey[len(internalKey)-4:]
	memberLen := binary.LittleEndian.Uint32(memberSizeBytes)
	memberStart := len(key) + 8
	memberEnd := memberStart + int(memberLen)
	member := make([]byte, memberLen)
	copy(member, internalKey[memberStart:memberEnd])
	return scoreBytes, member
}

// ZSet -----------------------------------------------------------

func (rds *DataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	unlock := rds.keyRWLocks.lock(key)
	defer unlock()
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
		if score == utils.DecodeFloat64FromBTree(value) {
			return false, nil
		}
	}

	batch := rds.db.GetBatch(false)
	defer rds.db.PutBatch(batch)

	if !exist {
		meta.size++
		if err = batch.Put(key, meta.encode()); err != nil {
			_ = batch.Rollback()
			return false, err
		}
	}

	if exist {
		oldKey := &zsetInternalKey{
			key:    key,
			member: member,
			score:  utils.DecodeFloat64FromBTree(value),
		}
		if err = batch.Delete(oldKey.encodeWithScore()); err != nil {
			_ = batch.Rollback()
			return false, err
		}
	}
	if err = batch.Put(zk.encodeWithMember(), utils.EncodeFloat64ForBTree(score)); err != nil {
		_ = batch.Rollback()
		return false, err
	}
	if err = batch.Put(zk.encodeWithScore(), nil); err != nil {
		_ = batch.Rollback()
		return false, err
	}
	if err = batch.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (rds *DataStructure) ZRem(key, member []byte) (bool, error) {
	unlock := rds.keyRWLocks.rlock(key)
	meta, err := rds.findMetaData(key, ZSet)
	unlock()
	if err != nil { // 暂时不支持
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	score, err := rds.ZScore(key, member)
	unlock = rds.keyRWLocks.lock(key)
	defer unlock()
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}

	zk := &zsetInternalKey{
		key:    key,
		member: member,
		score:  score,
	}

	batch := rds.db.GetBatch(false)
	defer rds.db.PutBatch(batch)

	if err := batch.Delete(zk.encodeWithScore()); err != nil {
		_ = batch.Rollback()
		return false, err
	}
	if err := batch.Delete(zk.encodeWithMember()); err != nil {
		_ = batch.Rollback()
		return false, err
	}

	meta.size--
	if err = batch.Put(key, meta.encode()); err != nil {
		_ = batch.Rollback()
		return false, err
	}
	if err = batch.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (rds *DataStructure) ZScore(key []byte, member []byte) (float64, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil { // 暂时不支持
		return math.Inf(-1), err
	}

	if meta.size == 0 {
		return math.Inf(-1), nil
	}

	zk := &zsetInternalKey{
		key:    key,
		member: member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return math.Inf(-1), err
	}

	return utils.DecodeFloat64FromBTree(value), nil
}

func (rds *DataStructure) ZCard(key []byte) (uint32, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return meta.size, nil
}

func (rds *DataStructure) ZRange(key []byte, start, end int, rev, withScores bool) ([]string, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return []string{}, nil
	}
	size := int(meta.size)
	start, end = adjustRangeIndices(start, end, size)
	if start > end || start >= size || end < 0 {
		return []string{}, nil
	}

	iterOptions := bitcask.DefaultIteratorOptions
	if rev {
		iterOptions.Reverse = true
	}
	iterOptions.Prefix = key
	iter := rds.db.NewIterator(iterOptions)
	defer iter.Close()

	var res []string
	count := 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		if !bytes.HasPrefix(item.Key, key) {
			continue
		}
		// 排除 key + member 的字段，留下key + scoreBuf(8) + member + len_member(4)
		if len(item.Value) != 0 {
			continue
		}

		score, member := decodeZSetInternalKey(key, item.Key)
		if count >= start && count <= end {
			res = append(res, string(member))
			if withScores {
				res = append(res, string(utils.DecodeFloat64FromBTreeToBytes(score)))
			}
		}

		count++
		if count > end {
			break
		}
	}
	return res, err
}

func (rds *DataStructure) ZRangeByScore(key []byte, min, max float64, rev, withScores bool, offset, count int) ([]string, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, ZSet)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return []string{}, nil
	}
	iterOptions := bitcask.DefaultIteratorOptions
	iterOptions.Prefix = key
	if rev {
		iterOptions.Reverse = true
	}
	iter := rds.db.NewIterator(iterOptions)
	defer iter.Close()

	var res []string
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		if !bytes.HasPrefix(item.Key, key) {
			continue
		}
		// 排除 key + member 的字段，留下key + scoreBuf(8) + member + len_member(4)
		if len(item.Value) != 0 {
			continue
		}
		scoreBytes, member := decodeZSetInternalKey(key, item.Key)
		score := utils.DecodeFloat64FromBTree(scoreBytes)
		if min <= score && score <= max {
			res = append(res, string(member))
			if withScores {
				res = append(res, string(utils.DecodeFloat64FromBTreeToBytes(scoreBytes)))
			}
		}
	}
	if withScores {
		count += count
		offset += offset
	}
	res = res[offset:]
	if count != 0 {
		res = res[0:count]
	}
	return res, err
}

func (rds *DataStructure) ZRank(key, member []byte, rev bool) (int, error) {
	score, err := rds.ZScore(key, member)
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	if err != nil || score == math.Inf(-1) {
		return -1, err
	}
	iterOptions := bitcask.DefaultIteratorOptions
	iterOptions.Prefix = key
	var sign float64 = 1
	if rev {
		iterOptions.Reverse = true
		sign = -1
	}
	iter := rds.db.NewIterator(iterOptions)
	defer iter.Close()
	rank := 0
	targetFound := false
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		if !bytes.HasPrefix(item.Key, key) {
			continue
		}
		if len(item.Value) != 0 {
			continue
		}
		curScoreBytes, curMember := decodeZSetInternalKey(key, item.Key)
		curScore := utils.DecodeFloat64FromBTree(curScoreBytes)
		if sign*curScore < sign*score {
			rank++
		} else if curScore == score {
			if bytes.Equal(curMember, member) {
				targetFound = true
				break
			}
		} else {
			break
		}
	}
	if !targetFound {
		return -1, nil
	}
	return rank, nil
}
