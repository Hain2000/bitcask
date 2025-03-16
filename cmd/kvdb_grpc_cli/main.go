package main

import (
	"bitcask/protocol/grpc/kvdb"
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := kvdb.NewKVServiceClient(conn)

	// 调用 Put
	putResp, err := client.Put(context.Background(), &kvdb.PutRequest{
		Key:   []byte("name"),
		Value: []byte("Alice"),
	})
	fmt.Println(putResp, err)
	// 调用 Get
	getResp, err := client.Get(context.Background(), &kvdb.GetRequest{
		Key: []byte("name"),
	})
	fmt.Println(getResp, err)
}
