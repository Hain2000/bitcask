package bitcask

import "errors"

var (
	ErrKeyIsEmpty             = errors.New("the key is empty")
	ErrIndexUpdateFailed      = errors.New("failed to update index")
	ErrKeyNotFound            = errors.New("key not found in database")
	ErrDataFileNotFound       = errors.New("data file not found")
	ErrDataDirectoryCorrupted = errors.New("the data directory maybe corrupted")
	ErrExceedMaxBatchNum      = errors.New("exceed the max batch num")
	ErrMerageIsProgress       = errors.New("merge is in progress, try again later")
)
