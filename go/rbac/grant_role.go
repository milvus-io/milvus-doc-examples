package rbac

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func GrantRole() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	err = client.GrantRole(ctx, milvusclient.NewGrantRoleOption("user_1", "role_a"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	user, err := client.DescribeUser(ctx, milvusclient.NewDescribeUserOption("user_1"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println(user)

	err = client.RevokeRole(ctx, milvusclient.NewRevokeRoleOption("user_1", "role_a"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	user, err = client.DescribeUser(ctx, milvusclient.NewDescribeUserOption("user_1"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println(user)
}
