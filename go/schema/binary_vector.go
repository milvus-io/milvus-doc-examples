package schema

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func BinaryVector() {
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
		WithIsAutoID(true).
		WithIsPrimaryKey(true).
		WithMaxLength(100),
	).WithField(entity.NewField().
		WithName("binary_vector").
		WithDataType(entity.FieldTypeBinaryVector).
		WithDim(128),
	)

	idx := index.NewAutoIndex(entity.HAMMING)
	indexOption := milvusclient.NewCreateIndexOption("my_binary_collection", "binary_vector", idx)

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_binary_collection", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_binary_collection").
		WithBinaryVectorColumn("binary_vector", 128, [][]byte{
			{0b10011011, 0b01010100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			{0b10011011, 0b01010101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		}))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("my_binary_collection"))
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

	queryVector := []byte{0b10011011, 0b01010100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	annSearchParams := index.NewCustomAnnParam()
	annSearchParams.WithExtraParam("nprobe", 10)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_binary_collection", // collectionName
		5,                      // limit
		[]entity.Vector{entity.BinaryVector(queryVector)},
	).WithConsistencyLevel(entity.ClStrong).
		WithANNSField("binary_vector").
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

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_binary_collection"))
}
