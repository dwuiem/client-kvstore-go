package main

import (
	store "client-kvstore-go"
	"context"
	"fmt"
)

func main() {
	client, err := store.NewKVStoreClient(
		"admin",
		"password",
		[]string{"localhost:8090", "localhost:8091", "localhost:8092"})
	if err != nil {
		panic(err)
	}
	err = client.Put(context.Background(), "key1", "value1", 10000)
	if err != nil {
		panic(err)
	}
	value, err := client.ConsistentGet(context.Background(), "key1")
	if err != nil {
		panic(err)
	}

	fmt.Println(value)
	defer func(client *store.KVStoreClient) {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}(client)
}
