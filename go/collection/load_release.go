package collection

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func Load() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	collectionName := "customized_setup_1"
	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	indexOptions := []milvusclient.CreateIndexOption{
		milvusclient.NewCreateIndexOption(collectionName, "my_vector", index.NewAutoIndex(entity.COSINE)).WithIndexName("my_vector"),
		milvusclient.NewCreateIndexOption(collectionName, "my_id", index.NewAutoIndex(entity.COSINE)).WithIndexName("my_id"),
	}

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).WithIndexOptions(indexOptions...))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("customized_setup_1"))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	// sync wait collection to be loaded
	err = loadTask.Await(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
}

func LoadFields() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	defer client.Close(ctx)

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("customized_setup_1").
		WithLoadFields("my_id", "my_vector"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	// sync wait collection to be loaded
	err = loadTask.Await(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("customized_setup_1"))
}

func Release() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	defer client.Close(ctx)

	err = client.ReleaseCollection(ctx, milvusclient.NewReleaseCollectionOption("customized_setup_1"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
}
