package main

import (
	"github.com/Hain2000/bitcask"
	"github.com/Hain2000/bitcask/protocol/resp"
	bitcaskredis "github.com/Hain2000/bitcask/redis"
	"log"
)

func main() {
	store, err := bitcaskredis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	server := resp.NewServer(store)
	log.Println("Server running at :6379")
	if err := server.Start(":6379"); err != nil {
		log.Fatal(err)
	}
}
