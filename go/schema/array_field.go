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

func ArrayField() {
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
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true),
	).WithField(entity.NewField().
		WithName("embedding").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(3),
	).WithField(entity.NewField().
		WithName("tags").
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeVarChar).
		WithMaxCapacity(10).
		WithMaxLength(65535).
		WithNullable(true),
	).WithField(entity.NewField().
		WithName("ratings").
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeInt64).
		WithMaxCapacity(5).
		WithNullable(true),
	)

	indexOpt1 := milvusclient.NewCreateIndexOption("my_collection", "tags", index.NewInvertedIndex())
	indexOpt2 := milvusclient.NewCreateIndexOption("my_collection", "embedding", index.NewAutoIndex(entity.COSINE))

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("my_collection", schema).
		WithIndexOptions(indexOpt1, indexOpt2))
	if err != nil {
		fmt.Println(err.Error())
		// handler err
	}

	column1, _ := column.NewNullableColumnVarCharArray("tags",
		[][]string{{"pop", "rock", "classic"}},
		[]bool{true, false, false})
	column2, _ := column.NewNullableColumnInt64Array("ratings",
		[][]int64{{5, 4, 3}, {4, 5}, {9, 5}},
		[]bool{true, true, true})

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithInt64Column("pk", []int64{1, 2, 3}).
		WithFloatVectorColumn("embedding", 3, [][]float32{
			{0.12, 0.34, 0.56},
			{0.78, 0.91, 0.23},
			{0.18, 0.11, 0.23},
		}).WithColumns(column1, column2))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	util.FlushLoadCollection(client, "my_collection")

	filter := "tags IS NOT NULL"
	rs, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter(filter).
		WithOutputFields("tags", "ratings", "pk"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("pk", rs.GetColumn("pk").FieldData().GetScalars())
	fmt.Println("tags", rs.GetColumn("tags").FieldData().GetScalars())
	fmt.Println("ratings", rs.GetColumn("ratings").FieldData().GetScalars())

	filter = "ratings[0] > 4"
	rs, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter(filter).
		WithOutputFields("tags", "ratings", "pk"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("pk", rs.GetColumn("pk"))
	fmt.Println("tags", rs.GetColumn("tags"))
	fmt.Println("ratings", rs.GetColumn("ratings"))

	queryVector := []float32{0.3, -0.6, 0.1}
	filter = "tags[0] == \"pop\""

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 10)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		5,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("embedding").
		WithFilter(filter).
		WithOutputFields("tags", "ratings", "embedding").
		WithAnnParam(annParam))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("tags", resultSet.GetColumn("tags").FieldData().GetScalars())
		fmt.Println("ratings", resultSet.GetColumn("ratings").FieldData().GetScalars())
		fmt.Println("embedding", resultSet.GetColumn("embedding").FieldData().GetVectors())
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection"))
}
