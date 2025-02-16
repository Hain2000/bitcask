package engine

type KvStore interface {
	Put(string, string) error
	Get(string) (string, error)
	Delete(string) error
	DumpPrefixKey(string, bool) (map[string]string, error)
	PutBytesKv(k []byte, v []byte) error
	DeleteBytesK(k []byte) error
	GetBytesValue(k []byte) ([]byte, error)
	SeekPrefixLast(prefix []byte) ([]byte, []byte, error)
	SeekPrefixFirst(prefix string) ([]byte, []byte, error)
	DelPrefixKeys(prefix string) error
	SeekPrefixKeyIdMax(prefix []byte) (uint64, error)
	FlushDB()
}

//func EngineFactory(name string, dbPath string) KvStore {
//	switch name {
//	case "leveldb":
//		levelDB, err := MakeLevelDBKvStore(dbPath)
//		if err != nil {
//			panic(err)
//		}
//		return levelDB
//	default:
//		panic("No such engine type support")
//	}
//}
