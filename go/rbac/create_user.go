package rbac

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func CreateUser() {
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

	err = client.CreateUser(ctx, milvusclient.NewCreateUserOption("user_1", "P@ssw0rd"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.UpdatePassword(ctx, milvusclient.NewUpdatePasswordOption("user_1", "P@ssw0rd", "NewP@ssw0rd"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	users, err := client.ListUsers(ctx, milvusclient.NewListUserOption())
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println(users)

	err = client.DropRole(ctx, milvusclient.NewDropRoleOption("role_a"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.CreateRole(ctx, milvusclient.NewCreateRoleOption("role_a"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	roles, err := client.ListRoles(ctx, milvusclient.NewListRoleOption())
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println(roles)
}
