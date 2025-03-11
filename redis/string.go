package redis

import (
	"bitcask"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (rds *DataStructure) Set(key []byte, value []byte, ttl time.Duration) error {
	if value == nil {
		return nil
	}
	encValue := make([]byte, 1+len(value))
	encValue[0] = String
	copy(encValue[1:], value)
	if ttl != 0 {
		return rds.db.PutWithTTL(key, encValue, ttl)
	}
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
	return encValue[1:], nil
}

func (rds *DataStructure) MSet(values []string) (int, error) {
	if len(values)%2 != 0 {
		return 0, errors.New("mset operation requires even number of arguments")
	}
	batch := rds.db.GetBatch(false)
	defer rds.db.PutBatch(batch)
	var errs []string
	n := 0
	for i := 0; i < len(values); i += 2 {
		k := []byte(values[i])
		v := []byte(values[i+1])
		encValue := make([]byte, 1+len(v))
		encValue[0] = String
		copy(encValue[1:], v)
		err := batch.Put(k, encValue)
		if err != nil {
			errs = append(errs, fmt.Sprintf("error getting key %s: %v", k, err))
		} else {
			n++
		}
	}
	err := batch.Commit()
	if err != nil {
		n = 0
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return n, errors.New(strings.Join(errs, "; "))
	}
	return n, nil
}

func (rds *DataStructure) MGet(keys []string) ([]string, error) {
	batch := rds.db.GetBatch(true)
	defer func() {
		_ = batch.Commit()
		rds.db.PutBatch(batch)
	}()
	res := make([]string, 0)
	var errs []string
	for i := 0; i < len(keys); i++ {
		k := []byte(keys[i])
		encValue, err := batch.Get(k)
		if err != nil {
			if !errors.Is(err, bitcask.ErrKeyNotFound) {
				errs = append(errs, fmt.Sprintf("error getting key %s: %v", k, err))
			}
			continue
		}
		dataType := encValue[0]
		if dataType != String {
			continue
		}
		res = append(res, string(encValue[1:]))
	}
	if len(errs) > 0 {
		return res, fmt.Errorf("multiple errors occurred: %s", strings.Join(errs, "; "))
	}
	return res, nil
}

func (rds *DataStructure) IncrBy(key []byte, value int64) (int64, error) {
	val, err := rds.db.Get(key)
	if err != nil {
		if errors.Is(err, bitcask.ErrKeyNotFound) {
			if err := rds.db.Put(key, []byte(strconv.FormatInt(value, 10))); err != nil {
				return 0, err
			}
			return value, nil
		}
		return 0, err
	}
	current, err := strconv.ParseInt(string(val), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer")
	}
	res := current + value
	if err := rds.db.Put(key, []byte(strconv.FormatInt(res, 10))); err != nil {
		return 0, err
	}
	return res, nil
}

func (rds *DataStructure) Incr(key []byte) (int64, error) {
	return rds.IncrBy(key, 1)
}

func (rds *DataStructure) Decr(key []byte) (int64, error) {
	return rds.IncrBy(key, -1)
}

func (rds *DataStructure) SetNX(key []byte, value []byte) error {
	var exist bool
	var err error
	if exist, err = rds.db.Exist(key); err != nil {
		return err
	}
	if exist {
		return nil
	}
	return rds.Set(key, value, 0)
}
