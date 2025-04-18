package search

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func TextMatch() {
	createCollectionText()
	defer util.DropCollection("my_collection")

	textMatchSearch()
}

func createCollectionText() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	analyzerParams := map[string]any{"type": "english"}

	schema := entity.NewSchema().WithDynamicFieldEnabled(false)
	schema.WithField(entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true).
		WithIsAutoID(true),
	).WithField(entity.NewField().
		WithName("text").
		WithDataType(entity.FieldTypeVarChar).
		WithEnableAnalyzer(true).
		WithEnableMatch(true).
		WithAnalyzerParams(analyzerParams).
		WithMaxLength(1000),
	).WithField(entity.NewField().
		WithName("embeddings").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(5),
	)

	indexOption := milvusclient.NewCreateIndexOption("my_collection", "embeddings",
		index.NewAutoIndex(entity.MetricType(entity.L2)))

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_collection", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithVarcharColumn("text", []string{
			"this is keyword1.",
			"no keyword.",
			"keyword1 and keyword2.",
		}).
		WithFloatVectorColumn("embeddings", 5, [][]float32{
			{0.1, 0.2, 0.3, 0.4, 0.5},
			{0.2, 0.3, 0.4, 0.5, 0.6},
			{0.3, 0.4, 0.5, 0.6, 0.7},
		}))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	util.FlushLoadCollection(client, "my_collection")
}

func textMatchSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	queryVector := []float32{0.1, 0.2, 0.3, 0.5, 0.7}
	filter := "TEXT_MATCH(text, 'keyword1 keyword2')"

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		10,              // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("embeddings").
		WithFilter(filter).
		WithOutputFields("id", "text"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("text: ", resultSet.GetColumn("text").FieldData().GetScalars())
	}

	filter = "TEXT_MATCH(text, 'keyword1') and TEXT_MATCH(text, 'keyword2')"
	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter(filter).
		WithOutputFields("id", "text"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("text: ", resultSet.GetColumn("text").FieldData().GetScalars())
}
