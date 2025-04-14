package redis

import (
	"errors"
	bitcask2 "github.com/Hain2000/bitcask"
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

type keyRWLock struct {
	locks sync.Map // map[string]*sync.RWMutex
}

func (krwl *keyRWLock) rlock(key []byte) func() {
	k := string(key)
	actual, _ := krwl.locks.LoadOrStore(k, &sync.RWMutex{})
	rw := actual.(*sync.RWMutex)
	rw.RLock()
	return func() { rw.RUnlock() }
}

func (krwl *keyRWLock) lock(key []byte) func() {
	k := string(key)
	actual, _ := krwl.locks.LoadOrStore(k, &sync.RWMutex{})
	rw := actual.(*sync.RWMutex)
	rw.Lock()
	return func() { rw.Unlock() }
}

// DataStructure Redis数据结构服务
type DataStructure struct {
	db         *bitcask2.DB
	listLock   sync.Mutex
	keyRWLocks keyRWLock
}

func NewRedisDataStructure(options bitcask2.Options) (*DataStructure, error) {
	db, err := bitcask2.Open(options)
	if err != nil {
		return nil, err
	}
	return &DataStructure{db: db}, nil
}

func (rds *DataStructure) findMetaData(key []byte, dataType redisType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && !errors.Is(err, bitcask2.ErrKeyNotFound) {
		return nil, err
	}

	var meta *metadata
	if errors.Is(err, bitcask2.ErrKeyNotFound) {
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
