package main

import (
	"bitcask"
	"bitcask/redis"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

func destroyDB(db *bitcask.DB) {
	_ = db.Close()
	_ = os.RemoveAll("./test_tmp")
}

type hashInternalKey struct {
	key   []byte
	filed []byte
}

func (hk *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hk.key)+len(hk.filed)+4)
	var index = 0
	copy(buf[index:index+len(hk.key)], hk.key)
	index += len(hk.key)
	size := make([]byte, 4)
	crcSum := crc32.ChecksumIEEE(hk.filed)
	binary.LittleEndian.PutUint32(size, crcSum)
	copy(buf[index:index+4], size)
	index += 4
	copy(buf[index:], hk.filed)
	return buf
}

func decodeFiled(buf, key []byte) []byte {
	filed := make([]byte, len(buf)-len(key)-4)
	copy(filed, buf[len(key)+4:])
	crcSum := binary.LittleEndian.Uint32(buf[len(key) : len(key)+4])
	if crc32.ChecksumIEEE(filed) != crcSum {
		return nil
	}
	return filed
}

func close(rds *redis.DataStructure, dir string) {
	_ = rds.Close()
	_ = os.RemoveAll(dir)
}

func main() {
	options := bitcask.DefaultOptions
	options.DirPath = "./test_tmp"
	//db, _ := bitcask.Open(options)
	//defer destroyDB(db)
	//
	//f1 := []string{"yyds1", "yyds2", "yyds3", "yyds4", "yyds5"}
	//v1 := []string{"v1", "v2", "v3", "v4", "v5"}
	//for i := 0; i < 5; i++ {
	//	hk := &hashInternalKey{
	//		key:   []byte("lcy"),
	//		filed: []byte(f1[i]),
	//	}
	//	_ = db.Put(hk.encode(), []byte(v1[i]))
	//}
	//
	//var keys []string
	//err := db.AscendKeys([]byte("lcy"), true, func(k []byte) (bool, error) {
	//	ff := decodeFiled(k, []byte("lcy"))
	//	if ff != nil {
	//		keys = append(keys, string(ff))
	//	}
	//	return true, nil
	//})
	//if err != nil {
	//	panic(err)
	//}
	//fmt.Println(keys)

	rds, _ := redis.NewRedisDataStructure(options)
	defer close(rds, options.DirPath)
	mp1 := map[string]string{
		"field1": "value1",
		"field2": "value2",
		"field3": "value3",
	}
	_, err := rds.HMSet([]byte("lcy"), mp1)
	if err != nil {
		panic(err)
	}

	keys, err := rds.HKeys([]byte("lcy"))
	if err != nil {
		panic(err)
	}
	fmt.Println(keys)
}
