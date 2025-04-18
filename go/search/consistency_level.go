package search

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ConsistencyLevel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	schema := entity.NewSchema().WithDynamicFieldEnabled(true)
	schema.WithField(entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true).
		WithIsAutoID(true),
	).WithField(entity.NewField().
		WithName("vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(4),
	)

	idx := index.NewAutoIndex(index.MetricType(entity.IP))
	indexOption := milvusclient.NewCreateIndexOption("my_collection", "vector", idx)

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_collection", schema).
			WithIndexOptions(indexOption).
			WithConsistencyLevel(entity.ClStrong))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithFloatVectorColumn("vector", 4, [][]float32{
			{0.1, 0.2, 0.3, 0.7},
			{0.2, 0.3, 0.4, 0.8},
		}),
	)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
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

	queryVector := []float32{0.1, 0.2, 0.3, 0.7}

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		3,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithConsistencyLevel(entity.ClBounded).
		WithANNSField("vector"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
	}

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("color like \"red%\"").
		WithOutputFields("vector", "color").
		WithLimit(3).
		WithConsistencyLevel(entity.ClEventually))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("vector: ", resultSet.GetColumn("vector").FieldData().GetVectors())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection"))
}
