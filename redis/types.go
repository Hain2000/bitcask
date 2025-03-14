package redis

import (
	"bitcask"
	"errors"
	"sync"
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

type KeyRWLock struct {
	locks sync.Map // map[string]*sync.RWMutex
}

func (krwl *KeyRWLock) RLock(key []byte) func() {
	k := string(key)
	actual, _ := krwl.locks.LoadOrStore(k, &sync.RWMutex{})
	rw := actual.(*sync.RWMutex)
	rw.RLock()
	return func() { rw.RUnlock() }
}

func (krwl *KeyRWLock) Lock(key []byte) func() {
	k := string(key)
	actual, _ := krwl.locks.LoadOrStore(k, &sync.RWMutex{})
	rw := actual.(*sync.RWMutex)
	rw.Lock()
	return func() { rw.Unlock() }
}

// DataStructure Redis数据结构服务
type DataStructure struct {
	db         *bitcask.DB
	listLock   sync.Mutex
	keyRWLocks KeyRWLock
}

func NewRedisDataStructure(options bitcask.Options) (*DataStructure, error) {
	db, err := bitcask.Open(options)
	if err != nil {
		return nil, err
	}
	return &DataStructure{db: db}, nil
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

func adjustRangeIndices(start, end, size int) (int, int) {
	if start < 0 {
		start += size
	}
	if end < 0 {
		end += size
	}
	if start < 0 {
		start = 0
	}
	if end >= size {
		end = size - 1
	}
	return start, end
}
