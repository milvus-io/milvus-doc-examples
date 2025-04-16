package schema

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func AlterField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("pk").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true),
	).WithField(entity.NewField().
		WithName("embedding").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(3),
	).WithField(entity.NewField().
		WithName("varchar").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(100),
	).WithField(entity.NewField().
		WithName("array").
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeInt64).
		WithMaxCapacity(5),
	).WithField(entity.NewField().
		WithName("doc_chunk").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(100),
	)

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("my_collection", schema))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.AlterCollectionFieldProperty(ctx, milvusclient.NewAlterCollectionFieldPropertiesOption(
		"my_collection", "varchar").WithProperty(common.MaxLengthKey, 1024))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.AlterCollectionFieldProperty(ctx, milvusclient.NewAlterCollectionFieldPropertiesOption(
		"my_collection", "array").WithProperty(common.MaxCapacityKey, 64))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.AlterCollectionFieldProperty(ctx, milvusclient.NewAlterCollectionFieldPropertiesOption(
		"my_collection", "doc_chunk").WithProperty(common.MmapEnabledKey, true))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection"))
}
