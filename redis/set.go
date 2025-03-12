package redis

import (
	"bitcask"
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type setInternalKey struct {
	key    []byte
	member []byte
}

func (sk *setInternalKey) encode() []byte {
	buf := make([]byte, len(sk.key)+len(sk.member)+4)
	var index = 0
	copy(buf[index:index+len(sk.key)], sk.key)
	index += len(sk.key)
	size := make([]byte, 4)
	crcSum := crc32.ChecksumIEEE(sk.member)
	binary.LittleEndian.PutUint32(size, crcSum)
	copy(buf[index:index+4], size)
	index += 4
	copy(buf[index:], sk.member)
	return buf
}

func decodeMember(buf, key []byte) []byte {
	if len(buf)-len(key)-4 <= 0 {
		return nil
	}
	member := make([]byte, len(buf)-len(key)-4)
	copy(member, buf[len(key)+4:])
	crcSum := binary.LittleEndian.Uint32(buf[len(key) : len(key)+4])
	if crc32.ChecksumIEEE(member) != crcSum {
		return nil
	}
	return member
}

// Set -----------------------------------------------------------

func (rds *DataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	sk := &setInternalKey{
		key:    key,
		member: member,
	}

	var ok = false

	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		// 不存在就更新
		batch := rds.db.GetBatch(false)
		defer rds.db.PutBatch(batch)
		meta.size++
		if err = batch.Put(key, meta.encode()); err != nil {
			_ = batch.Rollback()
			return false, err
		}
		if err = batch.Put(sk.encode(), nil); err != nil {
			_ = batch.Rollback()
			return false, err
		}
		if err = batch.Commit(); err != nil {
			return false, err
		}
		ok = true
	}
	return ok, nil
}

func (rds *DataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	// set存在没有数据
	if meta.size == 0 {
		return false, nil
	}

	// 构造数据部分的key
	sk := &setInternalKey{
		key:    key,
		member: member,
	}

	_, err = rds.db.Get(sk.encode())
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, err
	}

	if errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}
	return true, nil
}

func (rds *DataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}
	// set存在没有数据
	if meta.size == 0 {
		return false, nil
	}

	// 构造数据部分的key
	sk := &setInternalKey{
		key:    key,
		member: member,
	}

	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}

	batch := rds.db.GetBatch(false)
	defer rds.db.PutBatch(batch)
	meta.size--
	if err = batch.Put(key, meta.encode()); err != nil {
		_ = batch.Rollback()
		return false, err
	}
	if err = batch.Delete(sk.encode()); err != nil {
		_ = batch.Rollback()
		return false, err
	}
	if err = batch.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (rds *DataStructure) SCard(key []byte) (uint32, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *DataStructure) SMembers(key []byte) ([]string, error) {
	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	var members []string
	err = rds.db.AscendKeys(key, true, func(k []byte) (bool, error) {
		filed := decodeMember(k, key)
		if filed != nil {
			members = append(members, string(filed))
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return members, nil
}

func (rds *DataStructure) SInter(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	members, err := rds.SMembers([]byte(keys[0]))
	if err != nil {
		return nil, err
	}

	if len(keys) == 1 {
		return members, nil
	}

	res := make(map[string]int)
	for _, member := range members {
		res[member] = 1
	}

	for _, key := range keys[1:] {
		otherMembers, err := rds.SMembers([]byte(key))
		if err != nil {
			return nil, err
		}
		for _, member := range otherMembers {
			if count := res[member]; count > 0 {
				res[member]++
			}
		}
	}

	inter := make([]string, 0)
	for member, count := range res {
		if count == len(keys) {
			inter = append(inter, member)
		}
	}

	return inter, nil
}

func (rds *DataStructure) SDiff(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	members, err := rds.SMembers([]byte(keys[0]))
	if err != nil {
		return nil, err
	}

	if len(keys) == 1 {
		return members, nil
	}
	res := make(map[string]struct{})
	for _, member := range members {
		res[member] = struct{}{}
	}
	for _, key := range keys[1:] {
		otherMembers, err := rds.SMembers([]byte(key))
		if err != nil {
			return nil, err
		}
		for _, member := range otherMembers {
			delete(res, member)
		}
	}
	diff := make([]string, 0, len(res))
	for member := range res {
		diff = append(diff, member)
	}
	return diff, nil
}

func (rds *DataStructure) SUnion(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	res := make(map[string]struct{})

	for _, key := range keys {
		members, err := rds.SMembers([]byte(key))
		if err != nil {
			return nil, err
		}
		for _, member := range members {
			res[member] = struct{}{}
		}
	}

	union := make([]string, 0, len(res))
	for member := range res {
		union = append(union, member)
	}
	return union, nil
}
