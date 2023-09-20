package fio

const DataFilePerm = 0644

type FileIOType = byte

const (
	StanderFIO FileIOType = iota
	MemoryMap
)

// IOManager 抽象IO管理器接口
type IOManager interface {
	Read([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Sync() error // 表示从内存缓冲区中的数据持久化到磁盘IO
	Close() error
	Size() (int64, error)
}

func NewIOManager(name string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case MemoryMap:
		return NewMMapIOManager(name)
	case StanderFIO:
		return NewFileIOManager(name)
	default:
		panic("unsupported io type")
	}
}
