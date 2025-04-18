package search

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func Query() {
	get()
	query()
	getPartition()
	queryPartition()
}

func get() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Get(ctx, milvusclient.NewQueryOption("my_collection").
		WithIDs(column.NewColumnInt64("id", []int64{0, 1, 2})).
		WithOutputFields("vector", "color"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("vector: ", resultSet.GetColumn("vector").FieldData().GetVectors())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
}

func query() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("color like \"red%\"").
		WithOutputFields("vector", "color"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("vector: ", resultSet.GetColumn("vector").FieldData().GetVectors())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
}

func getPartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Get(ctx, milvusclient.NewQueryOption("my_collection").
		WithPartitions("partitionA").
		WithIDs(column.NewColumnInt64("id", []int64{10, 11, 12})).
		WithOutputFields("vector", "color"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("vector: ", resultSet.GetColumn("vector").FieldData().GetVectors())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
}

func queryPartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithPartitions("partitionA").
		WithFilter("color like \"red%\"").
		WithOutputFields("vector", "color"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("vector: ", resultSet.GetColumn("vector").FieldData().GetVectors())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
}
