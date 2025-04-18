package rbac

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func DropUser() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	err = client.DropUser(ctx, milvusclient.NewDropUserOption("user_1"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.DropRole(ctx, milvusclient.NewDropRoleOption("role_a"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
}
