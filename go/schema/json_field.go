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

func JsonField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

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
		WithName("metadata").
		WithDataType(entity.FieldTypeJSON),
	)

	jsonIndex1 := index.NewJSONPathIndex(index.Inverted, "varchar", `metadata["product_info"]["category"]`)
	jsonIndex2 := index.NewJSONPathIndex(index.Inverted, "double", `metadata["price"]`)
	indexOpt1 := milvusclient.NewCreateIndexOption("my_json_collection", "metadata", jsonIndex1)
	indexOpt2 := milvusclient.NewCreateIndexOption("my_json_collection", "metadata", jsonIndex2)

	vectorIndex := index.NewAutoIndex(entity.COSINE)
	indexOpt := milvusclient.NewCreateIndexOption("my_json_collection", "embedding", vectorIndex)

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("my_json_collection", schema).
		WithIndexOptions(indexOpt1, indexOpt2, indexOpt))
	if err != nil {
		fmt.Println(err.Error())
		// handler err
	}

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_json_collection").
		WithInt64Column("pk", []int64{1, 2, 3, 4}).
		WithFloatVectorColumn("embedding", 3, [][]float32{
			{0.12, 0.34, 0.56},
			{0.56, 0.78, 0.90},
			{0.91, 0.18, 0.23},
			{0.56, 0.38, 0.21},
		}).WithColumns(
		column.NewColumnJSONBytes("metadata", [][]byte{
			[]byte(`{
        "product_info": {"category": "electronics", "brand": "BrandA"},
        "price": 99.99,
        "in_stock": True,
        "tags": ["summer_sale"]
    }`),
			[]byte(`null`),
			[]byte(`null`),
			[]byte(`"metadata": {
        "product_info": {"category": None, "brand": "BrandB"},
        "price": 59.99,
        "in_stock": None
    }`),
		}),
	))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("my_json_collection"))
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

	rs, err := client.Query(ctx, milvusclient.NewQueryOption("my_json_collection").
		WithFilter("metadata is not null").
		WithOutputFields("metadata", "pk"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("pk", rs.GetColumn("pk").FieldData().GetScalars())
	fmt.Println("metadata", rs.GetColumn("metadata").FieldData().GetScalars())

	queryVector := []float32{0.3, -0.6, -0.1}

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 10)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_json_collection", // collectionName
		5,                    // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithConsistencyLevel(entity.ClStrong).
		WithANNSField("embedding").
		WithOutputFields("metadata").
		WithAnnParam(annParam))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("metadata", resultSet.GetColumn("metadata").FieldData().GetScalars())
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_json_collection"))
}
