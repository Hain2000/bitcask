package main

import (
	"context"
	"fmt"
	"github.com/Hain2000/bitcask/protocol/grpc/kvdb"
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

	client.Put(context.Background(), &kvdb.PutRequest{
		Key:   []byte("name1"),
		Value: []byte("lcy"),
	})

	client.Put(context.Background(), &kvdb.PutRequest{
		Key:   []byte("name2"),
		Value: []byte("qwq"),
	})

	client.Put(context.Background(), &kvdb.PutRequest{
		Key:   []byte("name3"),
		Value: []byte("aqa"),
	})
	resp, err := client.Get(context.Background(), &kvdb.GetRequest{
		Key: []byte("name1"),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.Found, string(resp.Value))

	_, err = client.Delete(context.Background(), &kvdb.DeleteRequest{
		Key: []byte("name1"),
	})
	if err != nil {
		panic(err)
	}

	resp, err = client.Get(context.Background(), &kvdb.GetRequest{
		Key: []byte("name1"),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(resp.Found)
}
