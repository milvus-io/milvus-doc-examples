package collection

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ListPartitions() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions("my_collection", 5))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	partitionNames, err := client.ListPartitions(ctx, milvusclient.NewListPartitionOption("my_collection"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println(partitionNames)
}

func CreatePartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.CreatePartition(ctx, milvusclient.NewCreatePartitionOption("my_collection", "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	partitionNames, err := client.ListPartitions(ctx, milvusclient.NewListPartitionOption("my_collection"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println(partitionNames)
}

func CheckPartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	result, err := client.HasPartition(ctx, milvusclient.NewHasPartitionOption("my_collection", "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println(result)
}

func LoadPartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	task, err := client.LoadPartitions(ctx, milvusclient.NewLoadPartitionsOption("my_collection", "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	// sync wait collection to be loaded
	err = task.Await(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	state, err := client.GetLoadState(ctx, milvusclient.NewGetLoadStateOption("my_collection", "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(state)
}

func ReleasePartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.ReleasePartitions(ctx, milvusclient.NewReleasePartitionsOptions("my_collection", "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
}

func DropPartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.ReleasePartitions(ctx, milvusclient.NewReleasePartitionsOptions("my_collection", "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.DropPartition(ctx, milvusclient.NewDropPartitionOption("my_collection", "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	partitionNames, err := client.ListPartitions(ctx, milvusclient.NewListPartitionOption("my_collection"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(partitionNames)

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection"))
}
