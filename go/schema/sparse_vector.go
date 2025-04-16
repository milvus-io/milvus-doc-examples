package schema

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func SparseVector() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("pk").
		WithDataType(entity.FieldTypeVarChar).
		WithIsAutoID(true).
		WithIsPrimaryKey(true).
		WithMaxLength(100),
	).WithField(entity.NewField().
		WithName("sparse_vector").
		WithDataType(entity.FieldTypeSparseVector),
	)

	idx := index.NewSparseInvertedIndex(entity.IP, 0.2)
	indexOption := milvusclient.NewCreateIndexOption("my_sparse_collection", "sparse_vector", idx)

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_sparse_collection", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	v := make([]entity.SparseEmbedding, 0, 2)
	sparseVector1, _ := entity.NewSliceSparseEmbedding([]uint32{1, 100, 500}, []float32{0.5, 0.3, 0.8})
	v = append(v, sparseVector1)
	sparseVector2, _ := entity.NewSliceSparseEmbedding([]uint32{10, 200, 1000}, []float32{0.1, 0.7, 0.9})
	v = append(v, sparseVector2)
	column := column.NewColumnSparseVectors("sparse_vector", v)

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_sparse_collection").
		WithColumns(column))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("my_sparse_collection"))
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

	queryVector, _ := entity.NewSliceSparseEmbedding([]uint32{1, 50, 1000}, []float32{0.2, 0.4, 0.7})

	annSearchParams := index.NewCustomAnnParam()
	annSearchParams.WithExtraParam("drop_ratio_search", 0.2)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_sparse_collection", // collectionName
		3,                      // limit
		[]entity.Vector{entity.SparseEmbedding(queryVector)},
	).WithConsistencyLevel(entity.ClStrong).
		WithANNSField("sparse_vector").
		WithOutputFields("pk").
		WithAnnParam(annSearchParams))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("Pks: ", resultSet.GetColumn("pk").FieldData().GetScalars())
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_sparse_collection"))
}
