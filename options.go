package bitcask

type Options struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64  // 数据文件的大小
	SyncWrite    bool   // 每次写是否需要持久化
}
