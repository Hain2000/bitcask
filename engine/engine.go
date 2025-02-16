package engine

import (
	"bitcask"
	"encoding/binary"
	"errors"
	"strings"
)

type KvStorageEngine struct {
	Path string
	db   *bitcask.DB
}

func MakeStorageEngine(path string) (*KvStorageEngine, error) {
	opts := bitcask.DefaultOptions
	opts.DirPath = path
	db, err := bitcask.Open(opts)
	if err != nil {
		return nil, err
	}
	return &KvStorageEngine{Path: path, db: db}, nil
}

func (e *KvStorageEngine) Put(k string, v string) error {
	return e.db.Put([]byte(k), []byte(v))
}

func (e *KvStorageEngine) Get(k string) (string, error) {
	v, err := e.db.Get([]byte(k))
	if err != nil {
		return "", err
	}
	return string(v), err
}

func (e *KvStorageEngine) Delete(s string) error {
	return e.db.Delete([]byte(s))
}

func (e *KvStorageEngine) PutBytesKv(k []byte, v []byte) error {
	return e.db.Put(k, v)
}

func (e *KvStorageEngine) GetBytesValue(k []byte) ([]byte, error) {
	return e.db.Get(k)
}

func (e *KvStorageEngine) DeleteBytesK(k []byte) error {
	return e.db.Delete(k)
}

func (e *KvStorageEngine) DumpPrefixKey(prefix string, trimPrefix bool) (map[string]string, error) {
	kvs := make(map[string]string)
	iterOpts := bitcask.DefaultIteratorOptions
	iterOpts.Prefix = []byte(prefix)
	iter := e.db.NewIterator(iterOpts)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		k := string(iter.Key())
		if trimPrefix {
			k = strings.TrimPrefix(k, prefix)
		}
		if v, err := iter.Value(); err == nil {
			kvs[k] = string(v)
		} else {
			return kvs, err
		}
	}
	return kvs, nil
}

func (e *KvStorageEngine) SeekPrefixLast(prefix []byte) ([]byte, []byte, error) {
	iterOpts := bitcask.DefaultIteratorOptions
	iterOpts.Prefix = prefix
	iterOpts.Reverse = true
	iter := e.db.NewIterator(iterOpts)
	defer iter.Close()
	if iter.Valid() {
		k := iter.Key()
		if v, err := iter.Value(); err == nil {
			return k, v, nil
		}
	}
	return []byte{}, []byte{}, nil
}

func (e *KvStorageEngine) SeekPrefixFirst(prefix string) ([]byte, []byte, error) {
	iterOpts := bitcask.DefaultIteratorOptions
	iterOpts.Prefix = []byte(prefix)
	iter := e.db.NewIterator(iterOpts)
	defer iter.Close()
	if iter.Valid() {
		k := iter.Key()
		if v, err := iter.Value(); err == nil {
			return k, v, nil
		}
	}
	return []byte{}, []byte{}, errors.New("seek not find key")
}

func (e *KvStorageEngine) DelPrefixKeys(prefix string) error {
	iterOpts := bitcask.DefaultIteratorOptions
	iterOpts.Prefix = []byte(prefix)
	iter := e.db.NewIterator(iterOpts)
	defer iter.Close()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		err := e.db.Delete(iter.Key())
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *KvStorageEngine) SeekPrefixKeyIdMax(prefix []byte) (uint64, error) {
	iterOpts := bitcask.DefaultIteratorOptions
	iterOpts.Prefix = prefix
	iter := e.db.NewIterator(iterOpts)
	defer iter.Close()
	var maxKeyId uint64
	maxKeyId = 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		kBytes := iter.Key()
		KeyId := binary.LittleEndian.Uint64(kBytes[len(prefix):])
		if KeyId > maxKeyId {
			maxKeyId = KeyId
		}
	}
	return maxKeyId, nil
}

func (e *KvStorageEngine) FlushDB() {
	//TODO implement me
	panic("implement me")
}
