package search

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func BasicSearch() {
	singleVectorSearch()
	bulkVectorSearch()
	inPartitionSearch()
	outputFieldsSearch()
	offsetSearch()
	levelSearch()
	recallSearch()
}

func singleVectorSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		3,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("vector"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
	}
}

func bulkVectorSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	queryVectors := []entity.Vector{
		entity.FloatVector([]float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}),
		entity.FloatVector([]float32{0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104}),
	}

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		3,               // limit
		queryVectors,
	).WithANNSField("vector"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
	}
}

func inPartitionSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		3,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithPartitions("partitionA").
		WithANNSField("vector"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
	}
}

func outputFieldsSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		3,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("vector").
		WithOutputFields("color"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	}
}

func offsetSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		3,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("vector").
		WithOffset(10))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
	}
}

func levelSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		3,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("vector").
		WithSearchParam("level", "10"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
	}
}

func recallSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		3,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("vector").
		WithSearchParam("enable_recall_calculation", "true"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
	}
}
