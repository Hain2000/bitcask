package main

import (
	"encoding/binary"
	"fmt"
	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/redis"
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

func closeDB(rds *redis.DataStructure, dir string) {
	_ = rds.Close()
	_ = os.RemoveAll(dir)
}

func test1() {
	options := bitcask.DefaultOptions
	options.DirPath = "./test_tmp"
	rds, err := redis.NewRedisDataStructure(options)
	if err != nil {
		panic(err)
	}
	defer closeDB(rds, options.DirPath)
	test_data := map[string]float64{
		"aa":  3,
		"bb":  2,
		"cc":  7,
		"dd":  8,
		"ee":  1,
		"ff":  10,
		"aaa": 4,
		"aab": 3,
	}

	for k, v := range test_data {
		_, err := rds.ZAdd([]byte("lcy"), v, []byte(k))
		if err != nil {
			panic(err)
		}
	}

	n, err := rds.ZCard([]byte("lcy"))
	fmt.Println(n)

	s, err := rds.ZRange([]byte("lcy"), 0, -1, true, true)
	if err != nil {
		panic(err)
	}
	fmt.Println(s)
}

func main() {
	test1()
	//val := -123.456
	//bytes := utils.Float64ToBytes(val)
	//fmt.Println("Encoded bytes:", bytes) // 打印字节数组
	//fmt.Println(len(bytes))
	//decoded := utils.BytesToFloat64(bytes)
	//fmt.Println("Decoded float64:", decoded) // 输出应为 -123.456

}
