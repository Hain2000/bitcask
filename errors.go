package bitcask

import "errors"

var (
	ErrKeyIsEmpty            = errors.New("the key is empty")
	ErrKeyNotFound           = errors.New("key not found in database")
	ErrBatchCommitted        = errors.New("the batch is committed")
	ErrBatchRollbacked       = errors.New("the batch is rollbacked")
	ErrMerageIsProgress      = errors.New("merge is in progress, try again later")
	ErrDatabaseIsUsing       = errors.New("the database directory is used by another process")
	ErrNoEnoughSpaceForMerge = errors.New("no enough disk to merge")
	ErrDBClosed              = errors.New("database is closed")
	ErrReadOnlyBatch         = errors.New("read-only batch")
)
