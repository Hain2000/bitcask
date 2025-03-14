package redis

import (
	"bitcask"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"sync"
)

// Hash -----------------------------------------------------------
type hashInternalKey struct {
	key   []byte
	filed []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.filed)+4)
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)
	size := make([]byte, 4)
	crcSum := crc32.ChecksumIEEE(hk.filed)
	binary.LittleEndian.PutUint32(size, crcSum)
	copy(buf[index:index+4], size)
	index += 4
	copy(buf[index:], hk.filed)
	return buf
}

func decodeFiled(buf, key []byte) []byte {
	// 防止找到本体
	if len(buf)-len(key)-4 <= 0 {
		return nil
	}
	filed := make([]byte, len(buf)-len(key)-4)
	copy(filed, buf[len(key)+4:])
	crcSum := binary.LittleEndian.Uint32(buf[len(key) : len(key)+4])
	if crc32.ChecksumIEEE(filed) != crcSum {
		return nil
	}
	return filed
}

// HSet 返回bool表示操作有没有成功
func (rds *DataStructure) HSet(key, field, value []byte) (bool, error) {
	unlock := rds.keyRWLocks.lock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}

	// key field 字段
	hk := hashInternalKey{
		key:   key,
		filed: field,
	}
	encKey := hk.encode()

	// 先找找有没有这个字段
	var existField = true
	val, err := rds.db.Get(encKey)
	if err != nil {
		if !errors.Is(err, bitcask.ErrKeyNotFound) {
			return false, err
		}
		existField = false
	}

	if bytes.Equal(value, val) {
		return false, nil
	}

	wb := rds.db.GetBatch(false)
	defer rds.db.PutBatch(wb)

	if !existField {
		meta.size++ // 字段数量 +1
		if err = wb.Put(key, meta.encode()); err != nil {
			_ = wb.Rollback()
			return false, err
		}
	}
	if err = wb.Put(encKey, value); err != nil {
		_ = wb.Rollback()
		return false, err
	}
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (rds *DataStructure) HGet(key, field []byte) ([]byte, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:   key,
		filed: field,
	}
	return rds.db.Get(hk.encode())
}

func (rds *DataStructure) HDel(key, field []byte) (bool, error) {
	unlock := rds.keyRWLocks.lock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	hk := &hashInternalKey{
		key:   key,
		filed: field,
	}
	encKey := hk.encode()

	var exist = true
	if _, err = rds.db.Get(encKey); errors.Is(err, bitcask.ErrKeyNotFound) {
		exist = false
	}

	if exist {
		wb := rds.db.GetBatch(false)
		defer rds.db.PutBatch(wb)
		meta.size--
		if err = wb.Put(key, meta.encode()); err != nil {
			_ = wb.Rollback()
			return false, err
		}
		if err = wb.Delete(encKey); err != nil {
			_ = wb.Rollback()
			return false, err
		}
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}

func (rds *DataStructure) HMSet(key []byte, fields map[string]string) (int, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	n := 0
	var errs []string
	for field, value := range fields {
		if ok, err := rds.HSet(key, []byte(field), []byte(value)); err != nil {
			errs = append(errs, fmt.Sprintf("error getting key %s filed: %s : %v", key, field, err))
		} else if ok {
			n++
		}
	}
	if len(errs) > 0 {
		return n, fmt.Errorf(strings.Join(errs, "\n"))
	}
	return n, nil
}

func (rds *DataStructure) HMGet(key []byte, fields ...string) (map[string]string, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	res := make(map[string]string, len(fields))
	var errs []string
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, field := range fields {
		wg.Add(1)
		go func(f string) {
			defer wg.Done()
			v, err := rds.HGet(key, []byte(f))
			if err != nil {
				if !errors.Is(err, bitcask.ErrKeyNotFound) {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("error getting field %s: %v", f, err))
					mu.Unlock()
				}
			}
			strVal := string(v)
			mu.Lock()
			res[field] = strVal
			mu.Unlock()
		}(field)
	}
	wg.Wait()
	if len(errs) > 0 {
		return res, fmt.Errorf("multiple errors occurred: %s", strings.Join(errs, "; "))
	}
	return res, nil
}

func (rds *DataStructure) HExist(key, field []byte) (bool, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	hk := &hashInternalKey{
		key:   key,
		filed: field,
	}
	return rds.db.Exist(hk.encode())
}

func (rds *DataStructure) HKeys(key []byte) ([]string, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	var fields []string
	err = rds.db.AscendKeys(key, true, func(k []byte) (bool, error) {
		filed := decodeFiled(k, key)
		if filed != nil {
			fields = append(fields, string(filed))
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	return fields, nil
}

func (rds *DataStructure) HVals(key []byte) ([]string, error) {
	fileds, err := rds.HKeys(key)
	if err != nil {
		return nil, err
	}
	mp, err := rds.HMGet(key, fileds...)
	var res []string
	for _, field := range fileds {
		res = append(res, mp[field])
	}
	return res, err
}

func (rds *DataStructure) HGetAll(key []byte) (map[string]string, error) {
	fileds, err := rds.HKeys(key)
	if err != nil {
		return nil, err
	}
	return rds.HMGet(key, fileds...)
}

func (rds *DataStructure) HIncrBy(key, field []byte, incr int64) (int64, error) {
	v, err := rds.HGet(key, field)
	if err != nil {
		if !errors.Is(err, bitcask.ErrKeyNotFound) {
			return 0, err
		}
	}
	strVal := string(v)
	if v == nil {
		strVal = "0"
	}
	current, err := strconv.ParseInt(strVal, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer")
	}
	res := incr + current
	if _, err := rds.HSet(key, field, []byte(strconv.FormatInt(res, 10))); err != nil {
		return 0, err
	}
	return res, nil
}

func (rds *DataStructure) HSetNX(key, field, value []byte) (bool, error) {
	unlock := rds.keyRWLocks.rlock(key)
	meta, err := rds.findMetaData(key, Hash)
	unlock()
	if err != nil {
		return false, err
	}
	exist, err := rds.HExist(key, field)
	if err != nil {
		return false, err
	}
	if exist {
		return false, nil
	}

	unlock = rds.keyRWLocks.lock(key)
	defer unlock()
	// key field 字段
	hk := hashInternalKey{
		key:   key,
		filed: field,
	}
	encKey := hk.encode()

	wb := rds.db.GetBatch(false)
	defer rds.db.PutBatch(wb)

	meta.size++ // 字段数量 +1
	if err = wb.Put(key, meta.encode()); err != nil {
		_ = wb.Rollback()
		return false, err
	}

	if err = wb.Put(encKey, value); err != nil {
		_ = wb.Rollback()
		return false, err
	}
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
