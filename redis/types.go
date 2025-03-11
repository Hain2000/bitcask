package redis

import (
	"bitcask"
	"bitcask/utils"
	"errors"
)

type redisType = byte

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operator against a key holding the wrong kind of value")
)

const (
	String redisType = iota
	Hash
	Set
	List
	ZSet
)

// DataStructure Redis数据结构服务
type DataStructure struct {
	db *bitcask.DB
}

func NewRedisDataStructure(options bitcask.Options) (*DataStructure, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &DataStructure{db}, nil
}

func (rds *DataStructure) findMetaData(key []byte, dataType redisType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
		return nil, err
	}

	var meta *metadata
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		meta = &metadata{
			dataType: dataType,
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	} else {
		meta = decodeMetaData(metaBuf)
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
	}

	return meta, nil
}

func (rds *DataStructure) Close() error {
	return rds.db.Close()
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
		wb := rds.db.NewBatch(bitcask.DefaultBatchOptions)
		meta.size++
		wb.Put(key, meta.encode())
		wb.Put(sk.encode(), nil)
		if err = wb.Commit(); err != nil {
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

	wb := rds.db.NewBatch(bitcask.DefaultBatchOptions)
	meta.size--
	wb.Put(key, meta.encode())
	wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// List ------------------------------------------------

func (rds *DataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *DataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *DataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return 0, err
	}

	//
	lk := &listInternalKey{
		key: key,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新元数据和数据
	wb := rds.db.NewBatch(bitcask.DefaultBatchOptions)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	wb.Put(key, meta.encode())
	wb.Put(lk.encode(), element)
	if err = wb.Commit(); err != nil {
		return 0, err
	}
	return meta.size, nil
}

func (rds *DataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *DataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *DataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	lk := &listInternalKey{
		key: key,
	}
	if isLeft {
		lk.index = meta.head
	} else {
		lk.index = meta.tail - 1
	}
	element, err := rds.db.Get(lk.encode())
	if err != nil {
		return nil, err
	}

	// 更新元数据
	meta.size--
	if isLeft {
		meta.head++
	} else {
		meta.tail--
	}
	if err = rds.db.Put(key, meta.encode()); err != nil {
		return nil, err
	}

	return element, nil
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
