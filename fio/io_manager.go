package fio

const DataFilePerm = 0644

// IOManager 抽象IO管理器接口
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error // 表示从内存缓冲区中的数据持久化到磁盘IO
	Close() error
}
