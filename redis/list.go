package redis

import (
	"bitcask"
	"encoding/binary"
	"errors"
	"fmt"
	"golang.org/x/exp/rand"
	"sync"
	"time"
)

type listInternalKey struct {
	key   []byte
	index uint64
}

func (lk *listInternalKey) encode() []byte {
	buf := make([]byte, len(lk.key)+8)
	var idx = 0
	copy(buf[idx:idx+len(lk.key)], lk.key)
	idx += len(lk.key)
	binary.LittleEndian.PutUint64(buf[idx:], lk.index)
	return buf
}

// List ------------------------------------------------

func (rds *DataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *DataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

func (rds *DataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	unlock := rds.keyRWLocks.lock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return 0, err
	}
	oldSize := meta.size
	lk := &listInternalKey{
		key: key,
	}
	if isLeft {
		lk.index = meta.head - 1
	} else {
		lk.index = meta.tail
	}
	wb := rds.db.GetBatch(false)
	defer rds.db.PutBatch(wb)
	meta.size++
	if isLeft {
		meta.head--
	} else {
		meta.tail++
	}
	if err = wb.Put(key, meta.encode()); err != nil {
		_ = wb.Rollback()
		return oldSize, err
	}
	if err = wb.Put(lk.encode(), element); err != nil {
		_ = wb.Rollback()
		return oldSize, err
	}
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
	unlock := rds.keyRWLocks.lock(key)
	defer unlock()
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

func (rds *DataStructure) LRange(key []byte, start, end int) ([]string, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return []string{}, nil
	}
	size := int(meta.size)
	start, end = adjustRangeIndices(start, end, size)
	if start > end {
		return []string{}, nil
	}
	res := make([]string, end-start+1)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 16)
	for i := start; i <= end; i++ {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(resIdx int) {
			defer func() {
				wg.Done()
				<-semaphore
			}()
			select {
			case <-errCh:
				return
			default:
			}
			lk := &listInternalKey{
				key:   key,
				index: meta.head + uint64(i),
			}
			val, err := rds.db.Get(lk.encode())
			if err != nil {
				select {
				case errCh <- fmt.Errorf("index %d: %v", resIdx, err):
				default:
				}
				return
			}
			res[resIdx] = string(val)
		}(i - start)
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()
	if firstErr := <-errCh; firstErr != nil {
		return nil, firstErr
	}
	return res, nil
}

func (rds *DataStructure) LTrim(key []byte, start, end int) error {
	unlock := rds.keyRWLocks.lock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return err
	}
	if meta.size == 0 {
		return nil
	}
	size := int(meta.size)
	start, end = adjustRangeIndices(start, end, size)
	if start > end { // 全删了
		meta.size = 0
		meta.head = initialListMark
		meta.tail = initialListMark
		return rds.db.Put(key, meta.encode())
	}
	head := meta.head
	meta.size = uint32(end - start + 1)
	meta.head = head + uint64(start)
	meta.tail = head + uint64(end)
	return rds.db.Put(key, meta.encode())
}

func (rds *DataStructure) LLen(key []byte) (int, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return 0, err
	}
	return int(meta.size), nil
}

func (rds *DataStructure) bPopInner(key []byte, ttl time.Duration, isLeft bool) ([]byte, error) {
	deadline := time.Now().Add(ttl)
	baseSleep := 5 * time.Millisecond // 初始 sleep 时间
	maxSleep := 30 * time.Millisecond // 最大 sleep 时间
	for time.Now().Before(deadline) {
		rds.listLock.Lock()
		value, err := rds.popInner(key, isLeft)
		if err == nil {
			if value != nil {
				rds.listLock.Unlock()
				return value, nil
			}
		}
		if err != nil && !errors.Is(err, bitcask.ErrKeyNotFound) {
			rds.listLock.Unlock()
			return nil, err
		}
		rds.listLock.Unlock()
		sleepTime := baseSleep + time.Duration(rand.Intn(int(baseSleep))) // 5ms + [0,5]ms
		if sleepTime > maxSleep {
			sleepTime = maxSleep
		}
		time.Sleep(sleepTime)
		baseSleep *= 2 // 指数退避
		if baseSleep > maxSleep {
			baseSleep = maxSleep
		}
	}
	return nil, nil
}

func (rds *DataStructure) BLPop(key []byte, ttl time.Duration) ([]byte, error) {
	return rds.bPopInner(key, ttl, true)
}

func (rds *DataStructure) BRPop(key []byte, ttl time.Duration) ([]byte, error) {
	return rds.bPopInner(key, ttl, false)
}

// LInsert 遇到的第一个refvalue插入
func (rds *DataStructure) LInsert(key, refvalue, value []byte, before bool) (bool, error) {
	unlock := rds.keyRWLocks.rlock(key)
	meta, err := rds.findMetaData(key, List)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, err
	}
	unlock()
	vals, err := rds.LRange(key, 0, -1)
	unlock = rds.keyRWLocks.lock(key)
	defer unlock()
	if err != nil {
		return false, err
	}
	var idx uint64
	for i, v := range vals {
		if v == string(refvalue) {
			idx = uint64(i)
			break
		}
	}
	batch := rds.db.GetBatch(false)
	defer rds.db.PutBatch(batch)
	idx += meta.head
	if before {
		idx--
	}
	for i := meta.head; i <= idx; i++ {
		lk := &listInternalKey{
			key:   key,
			index: i,
		}
		if err := batch.Delete(lk.encode()); err != nil {
			_ = batch.Rollback()
			return false, err
		}
		lk.index--
		if err := batch.Put(lk.encode(), []byte(vals[i-meta.head])); err != nil {
			_ = batch.Rollback()
			return false, err
		}
	}
	meta.size++
	meta.head--
	lk := &listInternalKey{
		key:   key,
		index: idx,
	}
	if err = batch.Put(lk.encode(), value); err != nil {
		_ = batch.Rollback()
		return false, err
	}
	if err = batch.Put(key, meta.encode()); err != nil {
		_ = batch.Rollback()
		return false, err
	}
	if err = batch.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
