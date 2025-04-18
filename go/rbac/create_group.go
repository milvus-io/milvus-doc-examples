package rbac

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func CreateGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	err = client.DropPrivilegeGroup(ctx, milvusclient.NewDropPrivilegeGroupOption("privilege_group_1"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.CreatePrivilegeGroup(ctx, milvusclient.NewCreatePrivilegeGroupOption("privilege_group_1"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	privileges := []string{"Query", "Search"}
	err = client.AddPrivilegesToGroup(ctx, milvusclient.NewAddPrivilegesToGroupOption("privilege_group_1", privileges...))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = client.RemovePrivilegesFromGroup(ctx, milvusclient.NewRemovePrivilegesFromGroupOption("privilege_group_1", []string{"Search"}...))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	groups, err := client.ListPrivilegeGroups(ctx, milvusclient.NewListPrivilegeGroupsOption())
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, group := range groups {
		fmt.Println(group.GroupName, group.Privileges)
	}
}
