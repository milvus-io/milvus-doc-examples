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

func NumberField() {
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
		WithName("price").
		WithDataType(entity.FieldTypeFloat).
		WithNullable(true),
	).WithField(entity.NewField().
		WithName("age").
		WithDataType(entity.FieldTypeInt64).
		WithNullable(true).
		WithDefaultValueLong(18),
	)

	indexOption1 := milvusclient.NewCreateIndexOption("my_collection", "embedding",
		index.NewAutoIndex(index.MetricType(entity.IP)))
	indexOption2 := milvusclient.NewCreateIndexOption("my_collection", "age",
		index.NewInvertedIndex())

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_collection", schema).
			WithIndexOptions(indexOption1, indexOption2))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	column1, _ := column.NewNullableColumnFloat("price",
		[]float32{99.99, 59.99},
		[]bool{true, false, false, false, true, false})
	column2, _ := column.NewNullableColumnInt64("age",
		[]int64{25, 30, 45, 60},
		[]bool{true, true, false, true, false, true})

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithInt64Column("pk", []int64{1, 2, 3, 4, 5, 6}).
		WithFloatVectorColumn("embedding", 3, [][]float32{
			{0.1, 0.2, 0.3},
			{0.4, 0.5, 0.6},
			{0.2, 0.3, 0.1},
			{0.9, 0.1, 0.4},
			{0.8, 0.5, 0.3},
			{0.1, 0.6, 0.9},
		}).
		WithColumns(column1, column2),
	)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	util.FlushLoadCollection(client, "my_collection")

	filter := "age > 30"
	queryResult, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter(filter).
		WithOutputFields("pk", "age", "price"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("pk", queryResult.GetColumn("pk"))
	fmt.Println("age", queryResult.GetColumn("age"))
	fmt.Println("price", queryResult.GetColumn("price"))

	filter = "price is null"
	queryResult, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter(filter).
		WithOutputFields("pk", "age", "price"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("pk", queryResult.GetColumn("pk"))
	fmt.Println("age", queryResult.GetColumn("age"))
	fmt.Println("price", queryResult.GetColumn("price"))

	filter = "age == 18"
	queryResult, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter(filter).
		WithOutputFields("pk", "age", "price"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("pk", queryResult.GetColumn("pk"))
	fmt.Println("age", queryResult.GetColumn("age"))
	fmt.Println("price", queryResult.GetColumn("price"))

	queryVector := []float32{0.3, -0.6, 0.1}
	filter = "25 <= age <= 35"

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 10)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		5,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("embedding").
		WithFilter(filter).
		WithAnnParam(annParam).
		WithOutputFields("age", "price"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("age: ", resultSet.GetColumn("age"))
		fmt.Println("price: ", resultSet.GetColumn("price"))
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection"))
}
