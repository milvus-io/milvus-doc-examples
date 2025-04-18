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

func VarcharField() {
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
		WithName("varchar_field1").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(100).
		WithNullable(true).
		WithDefaultValueString("Unknown"),
	).WithField(entity.NewField().
		WithName("varchar_field2").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(200).
		WithNullable(true),
	)

	indexOption1 := milvusclient.NewCreateIndexOption("my_collection", "embedding",
		index.NewAutoIndex(index.MetricType(entity.IP)))
	indexOption2 := milvusclient.NewCreateIndexOption("my_collection", "varchar_field1",
		index.NewInvertedIndex())

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_collection", schema).
			WithIndexOptions(indexOption1, indexOption2))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	column1, _ := column.NewNullableColumnVarChar("varchar_field1",
		[]string{"Product A", "Product B", "Product C", "Unknown", ""},
		[]bool{true, true, false, true, false, true, true})
	column2, _ := column.NewNullableColumnVarChar("varchar_field2",
		[]string{"High quality product", "Exclusive deal", "Best seller"},
		[]bool{true, false, false, false, true, false, true})

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithInt64Column("pk", []int64{1, 2, 3, 4, 5, 6, 7}).
		WithFloatVectorColumn("embedding", 3, [][]float32{
			{0.1, 0.2, 0.3},
			{0.4, 0.5, 0.6},
			{0.2, 0.3, 0.1},
			{0.5, 0.7, 0.2},
			{0.6, 0.4, 0.8},
			{0.8, 0.5, 0.3},
			{0.8, 0.5, 0.3},
		}).
		WithColumns(column1, column2),
	)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	util.FlushLoadCollection(client, "my_collection")

	filter := "varchar_field1 == \"Product A\""
	queryResult, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter(filter).
		WithOutputFields("varchar_field1", "varchar_field2"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("varchar_field1", queryResult.GetColumn("varchar_field1").FieldData().GetScalars())
	fmt.Println("varchar_field2", queryResult.GetColumn("varchar_field2").FieldData().GetScalars())

	filter = "varchar_field2 is null"
	queryResult, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter(filter).
		WithOutputFields("varchar_field1", "varchar_field2"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("varchar_field1", queryResult.GetColumn("varchar_field1"))
	fmt.Println("varchar_field2", queryResult.GetColumn("varchar_field2"))

	filter = "varchar_field1 == \"Unknown\""
	queryResult, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter(filter).
		WithOutputFields("varchar_field1", "varchar_field2"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("varchar_field1", queryResult.GetColumn("varchar_field1"))
	fmt.Println("varchar_field2", queryResult.GetColumn("varchar_field2"))

	queryVector := []float32{0.3, -0.6, 0.1}
	filter = "varchar_field2 == \"Best seller\""

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 10)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		5,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("embedding").
		WithFilter(filter).
		WithAnnParam(annParam).
		WithOutputFields("varchar_field1", "varchar_field2"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("varchar_field1: ", resultSet.GetColumn("varchar_field1").FieldData().GetScalars())
		fmt.Println("varchar_field2: ", resultSet.GetColumn("varchar_field2").FieldData().GetScalars())
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection"))
}
