package redis

import "errors"

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
