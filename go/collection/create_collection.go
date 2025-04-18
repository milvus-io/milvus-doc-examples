package collection

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func CreateCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(false).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	indexOptions := []milvusclient.CreateIndexOption{
		milvusclient.NewCreateIndexOption("customized_setup_1", "my_vector", index.NewAutoIndex(entity.COSINE)),
		milvusclient.NewCreateIndexOption("customized_setup_1", "my_id", index.NewAutoIndex(entity.COSINE)),
	}

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("customized_setup_1", schema).WithIndexOptions(indexOptions...))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("customized_setup_1"))
}

func CreateCollectionWithoutIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("customized_setup_2", schema))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	state, err := client.GetLoadState(ctx, milvusclient.NewGetLoadStateOption("customized_setup_2"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(state.State)

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("customized_setup_2"))
}

func CreateCollectionWithShardNum() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("customized_setup_3", schema).
		WithShardNum(1))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("customized_setup_3"))
}

func CreateCollectionWithMmap() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("customized_setup_4", schema).
		WithProperty(common.MmapEnabledKey, true))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("customized_setup_4"))
}

func CreateCollectionWithTTL() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("customized_setup_5", schema).
		WithProperty(common.CollectionTTLConfigKey, true))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("customized_setup_5"))
}

func CreateCollectionWithConsistencyLevel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("customized_setup_6", schema).
		WithConsistencyLevel(entity.ClBounded))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("customized_setup_6"))
}
