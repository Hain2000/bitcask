package bitcask

import (
	"bitcask/data"
	"bitcask/index"
	"bytes"
	"log"
)

// Iterator 面向用户的迭代器
type Iterator struct {
	indexIter index.IndexIterator
	db        *DB
	options   IteratorOptions
	lastError error // 遇到的最后一个错误
}

type Item struct {
	Key   []byte
	Value []byte
}

func (db *DB) NewIterator(opts IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opts.Reverse)
	iterator := &Iterator{
		db:        db,
		indexIter: indexIter,
		options:   opts,
	}
	_ = iterator.skipToNext()
	return iterator
}

func (it *Iterator) Rewind() {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Rewind()
}

func (it *Iterator) Seek(key []byte) {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Seek(key)
}

func (it *Iterator) Next() {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Next()
	_ = it.skipToNext()
}

func (it *Iterator) Valid() bool {
	if it.db == nil || it.indexIter == nil {
		return false
	}
	return it.indexIter.Valid()
}

func (it *Iterator) Key() []byte {
	if it.db == nil || it.indexIter == nil || !it.Valid() {
		return nil
	}
	return it.indexIter.Key()
}

func (it *Iterator) Item() *Item {
	if it.db == nil || it.indexIter == nil || !it.Valid() {
		return nil
	}
	record := it.skipToNext()
	if record == nil {
		return nil
	}
	return &Item{
		Key:   record.Key,
		Value: record.Value,
	}
}

func (it *Iterator) Close() {
	if it.db == nil || it.indexIter == nil {
		return
	}
	it.indexIter.Close()
	it.indexIter = nil
	it.db = nil
}

// ForEach 需要传func() {}
func (it *Iterator) ForEach(f func()) {
	for it.Rewind(); it.Valid(); it.Next() {
		f()
	}
}

func (it *Iterator) Err() error {
	return it.lastError
}

func (it *Iterator) skipToNext() *data.LogRecord {
	prefixLen := len(it.options.Prefix)
	for it.indexIter.Valid() {
		key := it.indexIter.Key()
		if prefixLen > 0 {
			if prefixLen > len(key) || !bytes.Equal(key[:prefixLen], it.options.Prefix) {
				it.indexIter.Next()
				continue
			}
		}

		position := it.indexIter.Value()
		if position == nil {
			it.indexIter.Next()
			continue
		}

		chunk, err := it.db.dataFiles.Read(position)
		if err != nil {
			it.lastError = err
			if !it.options.ContinueOnError {
				it.Close()
				return nil
			}
			log.Printf("Error reading data file at key %q: %v", key, err)
			it.indexIter.Next()
			continue
		}

		record := data.DecodeLogRecord(chunk)
		if record.Type == data.LogRecordDeleted {
			it.indexIter.Next()
			continue
		}
		return record
	}
	return nil
}
