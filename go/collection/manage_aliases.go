package collection

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func CreateAlias() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions("my_collection_1", 5))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	err = client.CreateAlias(ctx, milvusclient.NewCreateAliasOption("my_collection_1", "bob"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.CreateAlias(ctx, milvusclient.NewCreateAliasOption("my_collection_1", "alice"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
}

func ListAliases() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	aliases, err := client.ListAliases(ctx, milvusclient.NewListAliasesOption("my_collection_1"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(aliases)
}

func DescribeAlias() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	alias, err := client.DescribeAlias(ctx, milvusclient.NewDescribeAliasOption("bob"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(alias)
}

func AlterAlias() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions("my_collection_2", 5))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println("collection created")

	err = client.AlterAlias(ctx, milvusclient.NewAlterAliasOption("alice", "my_collection_2"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	aliases, err := client.ListAliases(ctx, milvusclient.NewListAliasesOption("my_collection_1"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(aliases)

	aliases, err = client.ListAliases(ctx, milvusclient.NewListAliasesOption("my_collection_2"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(aliases)
}

func DropAlias() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	err = client.DropAlias(ctx, milvusclient.NewDropAliasOption("bob"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.DropAlias(ctx, milvusclient.NewDropAliasOption("alice"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection_1"))
	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection_2"))
}
