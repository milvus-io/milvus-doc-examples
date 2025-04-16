package collection

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func SetTTLForCreate() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}
	defer client.Close(ctx)

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	collectionName := "customized_setup_5"
	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).
		WithProperty(common.CollectionTTLConfigKey, 1209600)) //  TTL in seconds
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
}

func SetTTLForExisting() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	collectionName := "my_collection"
	err = client.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, 5))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	err = client.AlterCollectionProperties(ctx, milvusclient.NewAlterCollectionPropertiesOption(collectionName).WithProperty(common.CollectionTTLConfigKey, 60))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
}

func DropTTL() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	collectionName := "my_collection"
	err = client.DropCollectionProperties(ctx, milvusclient.NewDropCollectionPropertiesOption(collectionName, common.CollectionTTLConfigKey))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
}
