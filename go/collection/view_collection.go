package collection

import (
	"context"
	"fmt"
	"log"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ListCollections() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	collectionNames, err := client.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		log.Println(err.Error())
		// handle error
	}

	fmt.Println(collectionNames)
}

func DescribeCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	collectionName := "my_collection"
	collection, err := client.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collectionName))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	fmt.Println(collection)

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
}
