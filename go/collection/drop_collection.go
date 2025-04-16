package collection

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func DropCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.DropCollection(ctx, milvusclient.NewDropCollectionOption("customized_setup_2"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
}
