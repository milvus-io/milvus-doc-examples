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

	collectionName := "quick_setup"
	err = client.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, 5))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	// client.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
}

func QuickSetupCustomFields() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	collectionName := "custom_quick_setup"
	err = client.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, 5).
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

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
}
