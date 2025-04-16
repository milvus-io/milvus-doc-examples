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

	err = client.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions("quick_setup", 5))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	partitionNames, err := client.ListPartitions(ctx, milvusclient.NewListPartitionOption("quick_setup"))
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

	err = client.CreatePartition(ctx, milvusclient.NewCreatePartitionOption("quick_setup", "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	partitionNames, err := client.ListPartitions(ctx, milvusclient.NewListPartitionOption("quick_setup"))
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

	result, err := client.HasPartition(ctx, milvusclient.NewHasPartitionOption("quick_setup", "partitionA"))
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

	task, err := client.LoadPartitions(ctx, milvusclient.NewLoadPartitionsOption("quick_setup", "partitionA"))
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
}

func ReleasePartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.ReleasePartitions(ctx, milvusclient.NewReleasePartitionsOptions("quick_setup", "partitionA"))
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

	err = client.DropPartition(ctx, milvusclient.NewDropPartitionOption("quick_setup", "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("quick_setup"))
}
