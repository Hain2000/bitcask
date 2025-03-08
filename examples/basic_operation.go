package main

import (
	"bitcask"
	"bitcask/utils"
	"fmt"
	"os"
	"path/filepath"
)

func Basic() {
	opts := bitcask.DefaultOptions
	opts.DirPath = "/tmp/bitcaskdata"
	database, err := bitcask.Open(opts)
	if err != nil {
		panic(err)
	}

	err = database.Put([]byte("lcy"), []byte("114514"))
	if err != nil {
		panic(err)
	}

	val, err := database.Get([]byte("lcy"))
	if err != nil {
		panic(err)
	}
	// fmt.Println()
	fmt.Println("val =", string(val))

	err = database.Delete([]byte("lcy"))
	if err != nil {
		panic(err)
	}

	val2, err := database.Get([]byte("lcy"))
	if len(val2) == 0 {
		fmt.Println("val2 = null")
	} else {
		fmt.Println("val2 =", string(val2))
	}
}

func mergeDirPath(dirPath string) string {
	dir := filepath.Dir(filepath.Clean(dirPath))
	base := filepath.Base(dirPath)
	return filepath.Join(dir, base+"merge-test")
}

func destroyDB(db *bitcask.DB) {
	_ = db.Close()
	_ = os.RemoveAll("./test_tmp")
	_ = os.RemoveAll(mergeDirPath("./test_tmp"))
}

func main() {
	options := bitcask.DefaultOptions
	options.DirPath = "./test_tmp"
	db, _ := bitcask.Open(options)
	defer destroyDB(db)

	kvs := make(map[string][]byte)
	for i := 0; i < 100000; i++ {
		key := utils.GetTestKey(i)
		value := utils.RandomValue(128)
		kvs[string(key)] = value
		_ = db.Put(key, value)
	}

	if err := db.Merge(true); err != nil {
		fmt.Errorf("%+v\n", err) // 输出错误详情
	}
	fmt.Println("hello world")
}
