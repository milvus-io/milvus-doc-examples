package schema

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func DenseVector() {
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
		WithDataType(entity.FieldTypeVarChar).
		WithIsPrimaryKey(true).
		WithIsAutoID(true).
		WithMaxLength(100),
	).WithField(entity.NewField().
		WithName("dense_vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(4),
	)

	idx := index.NewAutoIndex(index.MetricType(entity.IP))
	indexOption := milvusclient.NewCreateIndexOption("my_dense_collection", "dense_vector", idx)

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_dense_collection", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_dense_collection").
		WithFloatVectorColumn("dense_vector", 4, [][]float32{
			{0.1, 0.2, 0.3, 0.7},
			{0.2, 0.3, 0.4, 0.8},
		}),
	)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("my_dense_collection"))
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

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 10)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_dense_collection", // collectionName
		5,                     // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithConsistencyLevel(entity.ClStrong).
		WithANNSField("dense_vector").
		WithOutputFields("pk").
		WithAnnParam(annParam))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("Pks: ", resultSet.GetColumn("pk").FieldData().GetScalars())
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_dense_collection"))
}
