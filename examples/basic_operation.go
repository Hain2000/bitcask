package main

import (
	"bitcask"
	"bitcask/raft"
	"fmt"
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

func main() {
	raft.PrintDebugLog("YYDS 12312321")
}
