package main

import (
	"fmt"
	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/utils"
	"log"
	"os"
	"time"
)

func main() {
	options := bitcask.DefaultOptions
	options.DirPath = "/tmp/test_tmp"

	defer func() {
		_ = os.RemoveAll(options.DirPath)
	}()

	db, err := bitcask.Open(options)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close()
	}()

	err = db.Put([]byte("k1"), []byte("v1"))
	if err != nil {
		panic(err)
	}

	val, err := db.Get([]byte("k1"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(val))

	err = db.Delete([]byte("k1"))
	if err != nil {
		panic(err)
	}

	batch := db.NewBatch(bitcask.DefaultBatchOptions)
	_ = batch.Put([]byte("k2"), []byte("v2"))
	val, _ = batch.Get([]byte("k2"))
	fmt.Println(string(val))
	_ = batch.Delete([]byte("k2"))
	_ = batch.Commit()
	// if you want to cancel batch, you must call rollback().
	// _= batch.Rollback()

	err = db.PutWithTTL([]byte("k3"), []byte("v3"), time.Second*5)
	if err != nil {
		panic(err)
	}
	ttl, err := db.TTL([]byte("k3"))
	if err != nil {
		panic(err)
	}
	fmt.Println(ttl.String())

	_ = db.Put([]byte("k4"), []byte("v4"))
	err = db.Expire([]byte("k4"), time.Second*2)
	if err != nil {
		panic(err)
	}
	ttl, err = db.TTL([]byte("k4"))
	if err != nil {
		log.Println(err)
	}
	fmt.Println(ttl.String())

	for i := 0; i < 100000; i++ {
		_ = db.Put([]byte(utils.GetTestKey(i)), utils.RandomValue(128))
	}
	// delete some data
	for i := 0; i < 100000/2; i++ {
		_ = db.Delete([]byte(utils.GetTestKey(i)))
	}

	fmt.Println(db.Stat().DiskSize)
	_ = db.Merge(true)
	fmt.Println(db.Stat().DiskSize)
}
