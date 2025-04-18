package collection

import (
	"context"
	"fmt"
	"log"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func QuickSetup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions("quick_setup", 5))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("quick_setup"))
}

func QuickSetupCustomFields() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions("custom_quick_setup", 5).
		WithPKFieldName("my_id").
		WithVarcharPK(true, 512).
		WithVectorFieldName("my_vector").
		WithMetricType(entity.L2).
		WithAutoID(true),
	)
	if err != nil {
		log.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("custom_quick_setup"))
}
