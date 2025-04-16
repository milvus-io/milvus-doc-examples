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

	indexOption := milvusclient.NewCreateIndexOption("user_profiles_null", "vector",
		index.NewAutoIndex(index.MetricType(entity.L2)))

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("user_profiles_null", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	column, _ := column.NewNullableColumnInt64("age",
		[]int64{30},
		[]bool{true, false, false})

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("user_profiles_null").
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

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("user_profiles_null"))
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

	queryVector := []float32{0.1, 0.2, 0.4, 0.3, 0.128}

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 16)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"user_profiles_null", // collectionName
		5,                    // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithConsistencyLevel(entity.ClStrong).
		WithANNSField("vector").
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

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("user_profiles_null"))
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

	indexOption := milvusclient.NewCreateIndexOption("user_profiles_default", "vector",
		index.NewAutoIndex(index.MetricType(entity.L2)))

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("user_profiles_default", schema).
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

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("user_profiles_default").
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

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("user_profiles_default"))
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

	queryVector := []float32{0.1, 0.2, 0.4, 0.3, 0.128}

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 16)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"user_profiles_default", // collectionName
		10,                      // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithConsistencyLevel(entity.ClStrong).
		WithANNSField("vector").
		WithAnnParam(annParam).
		WithFilter("age == 18").
		WithOutputFields("id", "age", "status"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("age: ", resultSet.GetColumn("age"))
		fmt.Println("status: ", resultSet.GetColumn("status"))
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("user_profiles_default"))
}
