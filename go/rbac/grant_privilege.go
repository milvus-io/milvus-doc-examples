package rbac

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func GrantPrivilige() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	err = client.GrantV2(ctx, milvusclient.NewGrantV2Option("role_a", "Search", "default", "collection_01"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.GrantV2(ctx, milvusclient.NewGrantV2Option("role_a", "privilege_group_1", "default", "collection_01"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.GrantV2(ctx, milvusclient.NewGrantV2Option("role_a", "ClusterReadOnly", "*", "*"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	role, err := client.DescribeRole(ctx, milvusclient.NewDescribeRoleOption("role_a"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println(role)

	err = client.RevokePrivilegeV2(ctx, milvusclient.NewRevokePrivilegeV2Option("role_a", "Search", "collection_01").
		WithDbName("default"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.RevokePrivilegeV2(ctx, milvusclient.NewRevokePrivilegeV2Option("role_a", "privilege_group_1", "collection_01").
		WithDbName("default"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.RevokePrivilegeV2(ctx, milvusclient.NewRevokePrivilegeV2Option("role_a", "ClusterReadOnly", "*").
		WithDbName("*"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	role, err = client.DescribeRole(ctx, milvusclient.NewDescribeRoleOption("role_a"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println(role)
}
