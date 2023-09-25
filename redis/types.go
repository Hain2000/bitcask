package redis

import (
	"bitcask"
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

//func (rds *DataStructure) SAdd(key, member []byte) (bool, error) {
//
//}
