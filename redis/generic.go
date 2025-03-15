package redis

import (
	"errors"
	"time"
)

func (rds *DataStructure) Del(key []byte) error {
	unlock := rds.keyRWLocks.lock(key)
	defer unlock()
	return rds.db.Delete(key)
}

func (rds *DataStructure) Type(key []byte) (redisType, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	encValue, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(encValue) == 0 {
		return 0, errors.New("value is null")
	}
	return encValue[0], nil
}

func (rds *DataStructure) TTL(key []byte) (float64, error) {
	unlock := rds.keyRWLocks.rlock(key)
	defer unlock()
	ttl, err := rds.db.TTL(key)
	if err != nil {
		return 0, err
	}
	return ttl.Seconds(), nil
}

func (rds *DataStructure) Expire(key []byte, ttl time.Duration) error {
	unlock := rds.keyRWLocks.lock(key)
	defer unlock()
	err := rds.db.Expire(key, ttl)
	if err != nil {
		return err
	}
	return nil
}
