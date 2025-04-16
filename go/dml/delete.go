package dml

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func Delete() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	_, err = client.Delete(ctx, milvusclient.NewDeleteOption("quick_setup").WithExpr("color in ['red_7025', 'purple_4976']"))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	_, err = client.Delete(ctx, milvusclient.NewDeleteOption("quick_setup").
		WithInt64IDs("id", []int64{18, 19}))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	_, err = client.Delete(ctx, milvusclient.NewDeleteOption("quick_setup").
		WithInt64IDs("id", []int64{18, 19}).
		WithPartition("partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("quick_setup"))
}
