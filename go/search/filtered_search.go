package search

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func FilteredSearch() {
	standardFilter()
	iterativeFilter()
}

func standardFilter() {
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
		5,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("vector").
		WithFilter("color like 'red%' and likes > 50").
		WithOutputFields("color", "likes"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
		fmt.Println("likes: ", resultSet.GetColumn("likes").FieldData().GetScalars())
	}
}

func iterativeFilter() {
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
		5,               // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithANNSField("vector").
		WithFilter("color like 'red%' and likes > 50").
		WithOutputFields("color", "likes").
		WithSearchParam("hints", "iterative_filter"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
		fmt.Println("likes: ", resultSet.GetColumn("likes").FieldData().GetScalars())
	}
}
