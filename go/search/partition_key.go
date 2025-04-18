package search

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func PartitionKey() {
	createCollectionKey()
	defer util.DropCollection("my_collection")
}

func createCollectionKey() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	schema := entity.NewSchema().WithDynamicFieldEnabled(false)
	schema.WithField(entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true),
	).WithField(entity.NewField().
		WithName("my_varchar").
		WithDataType(entity.FieldTypeVarChar).
		WithIsPartitionKey(true).
		WithMaxLength(512),
	).WithField(entity.NewField().
		WithName("vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(5),
	)

	indexOption := milvusclient.NewCreateIndexOption("my_collection", "vector",
		index.NewAutoIndex(entity.MetricType(entity.L2)))

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_collection", schema).
			WithIndexOptions(indexOption).
			WithProperty("partitionkey.isolation", true).
			WithNumPartitions(128))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithInt64Column("id", []int64{1, 2, 3}).
		WithVarcharColumn("my_varchar", []string{
			"aaa",
			"bbb",
			"ccc",
		}).
		WithFloatVectorColumn("vector", 5, [][]float32{
			{0.1, 0.2, 0.3, 0.4, 0.5},
			{0.2, 0.3, 0.4, 0.5, 0.6},
			{0.3, 0.4, 0.5, 0.6, 0.7},
		}))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	flushTask, err := client.Flush(ctx, milvusclient.NewFlushOption("my_collection"))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}
	err = flushTask.Await(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("my_collection"))
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
