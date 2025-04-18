package database

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ManageDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	dbs, err := client.ListDatabase(ctx, milvusclient.NewListDatabaseOption())
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(dbs)

	err = client.CreateDatabase(ctx, milvusclient.NewCreateDatabaseOption("my_database"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	db, err := client.DescribeDatabase(ctx, milvusclient.NewDescribeDatabaseOption("my_database"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(db)

	client, err = milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: "localhost:19530",
		APIKey:  "root:Milvus",
		DBName:  "my_database",
	})
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	err = client.UseDatabase(ctx, milvusclient.NewUseDatabaseOption("my_database"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	client, err = util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	err = client.DropDatabase(ctx, milvusclient.NewDropDatabaseOption("my_database"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
}
