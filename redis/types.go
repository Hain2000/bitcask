package redis

import (
	"bitcask"
	"bitcask/utils"
	"encoding/binary"
	"errors"
	"time"
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
	var exist = true
	if errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	} else {
		meta = decodeMetaData(metaBuf)
		// 还要判断数据类型
		if meta.dataType != dataType {
			// fmt.Println(meta.dataType)
			return nil, ErrWrongTypeOperation
		}
		// 判断过去事件
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}
	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}

func (rds *DataStructure) Close() error {
	return rds.db.Close()
}

// String -----------------------------------------------------------

func (rds *DataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)
	// 调用存储引擎接口写入
	return rds.db.Put(key, encValue)
}

func (rds *DataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}
	return encValue[index:], nil
}

// Hash -----------------------------------------------------------

// HSet 返回bool表示操作有没有成功
func (rds *DataStructure) HSet(key, field, value []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 Hash Key
	hk := hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	// 先查找是否存在
	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

	// 不存在则更新元数据
	if !exist {
		meta.size++ // k对应的值 +1
		wb.Put(key, meta.encode())
	}
	wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}

func (rds *DataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *DataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		filed:   field,
	}
	encKey := hk.encode()

	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		meta.size--
		wb.Put(key, meta.encode())
		wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}

// Set -----------------------------------------------------------

func (rds *DataStructure) SAdd(key, member []byte) (bool, error) {

	meta, err := rds.findMetaData(key, Set)
	if err != nil {
		return false, err
	}

	// 构造数据部分的key
	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok = false

	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		// 不存在就更新
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
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
		key:     key,
		version: meta.version,
		member:  member,
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
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); errors.Is(err, bitcask.ErrKeyNotFound) {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
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
		key:     key,
		version: meta.version,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}

	// 更新元数据和数据
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
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
		key:     key,
		version: meta.version,
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
		key:     key,
		version: meta.version,
		score:   score,
		member:  member,
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
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	if !exist {
		meta.size++
		wb.Put(key, meta.encode())
	}

	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: meta.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
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
		key:     key,
		version: meta.version,
		member:  member,
	}

	value, err := rds.db.Get(zk.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}
