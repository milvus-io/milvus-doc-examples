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

func NullableField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true),
	).WithField(entity.NewField().
		WithName("vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(5),
	).WithField(entity.NewField().
		WithName("age").
		WithDataType(entity.FieldTypeInt64).
		WithNullable(true),
	)

	indexOption := milvusclient.NewCreateIndexOption("my_collection", "vector",
		index.NewAutoIndex(index.MetricType(entity.L2)))

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_collection", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	column, _ := column.NewNullableColumnInt64("age",
		[]int64{30},
		[]bool{true, false, false})

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithInt64Column("id", []int64{1, 2, 3}).
		WithFloatVectorColumn("vector", 5, [][]float32{
			{0.1, 0.2, 0.3, 0.4, 0.5},
			{0.2, 0.3, 0.4, 0.5, 0.6},
			{0.3, 0.4, 0.5, 0.6, 0.7},
		}).
		WithColumns(column),
	)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	util.FlushLoadCollection(client, "my_collection")

	queryVector := []float32{0.1, 0.2, 0.4, 0.3, 0.5}

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 16)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		2,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("vector").
		WithAnnParam(annParam).
		WithOutputFields("id", "age"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("age: ", resultSet.GetColumn("age"))
	}

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("age >= 0").
		WithOutputFields("id", "age"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("age: ", resultSet.GetColumn("age").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("").
		WithLimit(10).
		WithOutputFields("id", "age"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("id: ", resultSet.GetColumn("id"))
	fmt.Println("age: ", resultSet.GetColumn("age"))

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection"))
}

func DefaultField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true),
	).WithField(entity.NewField().
		WithName("vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(5),
	).WithField(entity.NewField().
		WithName("age").
		WithDataType(entity.FieldTypeInt64).
		WithDefaultValueLong(18),
	).WithField(entity.NewField().
		WithName("status").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(10).
		WithDefaultValueString("active"),
	)

	indexOption := milvusclient.NewCreateIndexOption("my_collection", "vector",
		index.NewAutoIndex(index.MetricType(entity.L2)))

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_collection", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	column1, _ := column.NewNullableColumnInt64("age",
		[]int64{30, 25},
		[]bool{true, false, true, false})
	column2, _ := column.NewNullableColumnVarChar("status",
		[]string{"premium", "inactive"},
		[]bool{true, false, false, true})

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithInt64Column("id", []int64{1, 2, 3, 4}).
		WithFloatVectorColumn("vector", 5, [][]float32{
			{0.1, 0.2, 0.3, 0.4, 0.5},
			{0.2, 0.3, 0.4, 0.5, 0.6},
			{0.3, 0.4, 0.5, 0.6, 0.7},
			{0.4, 0.5, 0.6, 0.7, 0.8},
		}).
		WithColumns(column1, column2),
	)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	util.FlushLoadCollection(client, "my_collection")

	queryVector := []float32{0.1, 0.2, 0.4, 0.3, 0.5}

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 16)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		10,              // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("vector").
		WithFilter("age == 18").
		WithAnnParam(annParam).
		WithOutputFields("id", "age", "status"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("age: ", resultSet.GetColumn("age").FieldData().GetScalars())
		fmt.Println("status: ", resultSet.GetColumn("status").FieldData().GetScalars())
	}

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("age == 18").
		WithOutputFields("id", "age", "status"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("age: ", resultSet.GetColumn("age").FieldData().GetScalars())
	fmt.Println("status: ", resultSet.GetColumn("status").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("status == \"active\"").
		WithOutputFields("id", "age", "status"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("age: ", resultSet.GetColumn("age").FieldData().GetScalars())
	fmt.Println("status: ", resultSet.GetColumn("status").FieldData().GetScalars())

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection"))
}
