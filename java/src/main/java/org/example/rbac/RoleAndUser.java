package org.example.rbac;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.rbac.PrivilegeGroup;
import io.milvus.v2.service.rbac.request.*;
import io.milvus.v2.service.rbac.response.DescribeRoleResp;
import io.milvus.v2.service.rbac.response.DescribeUserResp;
import io.milvus.v2.service.rbac.response.ListPrivilegeGroupsResp;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RoleAndUser {
    private static final MilvusClientV2 client;
    static {
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build();

        client = new MilvusClientV2(connectConfig);
    }

    private static void createUser() {
        CreateUserReq createUserReq = CreateUserReq.builder()
                .userName("user_1")
                .password("P@ssw0rd")
                .build();

        client.createUser(createUserReq);
    }

    private static void updatePassword() {
        UpdatePasswordReq updatePasswordReq = UpdatePasswordReq.builder()
                .userName("user_1")
                .password("P@ssw0rd")
                .newPassword("NewP@ssw0rd")
                .build();
        client.updatePassword(updatePasswordReq);
    }

    private static void listUsers() {
        List<String> resp = client.listUsers();
        System.out.println(resp);
    }

    private static void createRole() {
        CreateRoleReq createRoleReq = CreateRoleReq.builder()
                .roleName("role_a")
                .build();

        client.createRole(createRoleReq);
    }

    private static void listRoles() {
        List<String> resp = client.listRoles();
        System.out.println(resp);
    }

    private static void createPrivilegeGroup() {
        client.createPrivilegeGroup(CreatePrivilegeGroupReq.builder()
                .groupName("privilege_group_1")
                .build());
    }

    private static void addPrivilegeToGroup() {
        client.addPrivilegesToGroup(AddPrivilegesToGroupReq.builder()
                .groupName("privilege_group_1")
                .privileges(Arrays.asList("Query", "Search"))
                .build());
    }

    private static void removePrivilegeFromGroup() {
        client.removePrivilegesFromGroup(RemovePrivilegesFromGroupReq.builder()
                .groupName("privilege_group_1")
                .privileges(Collections.singletonList("Search"))
                .build());
    }

    private static void listPrivilegeGroups() {
        ListPrivilegeGroupsResp resp = client.listPrivilegeGroups(ListPrivilegeGroupsReq.builder()
                .build());
        List<PrivilegeGroup> groups = resp.getPrivilegeGroups();
        System.out.println(groups);
    }

    private static void dropPrivilegeGroup() {
        client.dropPrivilegeGroup(DropPrivilegeGroupReq.builder()
                .groupName("privilege_group_1")
                .build());
    }

    public static void grantPrivilege() {
        client.grantPrivilegeV2(GrantPrivilegeReqV2.builder()
                .roleName("role_a")
                .privilege("Search")
                .collectionName("collection_01")
                .dbName("default")
                .build());

        client.grantPrivilegeV2(GrantPrivilegeReqV2.builder()
                .roleName("role_a")
                .privilege("privilege_group_1")
                .collectionName("collection_01")
                .dbName("default")
                .build());

        client.grantPrivilegeV2(GrantPrivilegeReqV2.builder()
                .roleName("role_a")
                .privilege("ClusterReadOnly")
                .collectionName("*")
                .dbName("*")
                .build());
    }

    public static void describeRole() {
        DescribeRoleReq describeRoleReq = DescribeRoleReq.builder()
                .roleName("role_a")
                .build();
        DescribeRoleResp resp = client.describeRole(describeRoleReq);
        List<DescribeRoleResp.GrantInfo> infos = resp.getGrantInfos();
        System.out.println(infos);
    }

    public static void revokePrivilege() {
        client.revokePrivilegeV2(RevokePrivilegeReqV2.builder()
                .roleName("role_a")
                .privilege("Search")
                .collectionName("collection_01")
                .dbName("default")
                .build());

        client.revokePrivilegeV2(RevokePrivilegeReqV2.builder()
                .roleName("role_a")
                .privilege("privilege_group_1")
                .collectionName("collection_01")
                .dbName("default")
                .build());

        client.revokePrivilegeV2(RevokePrivilegeReqV2.builder()
                .roleName("role_a")
                .privilege("ClusterReadOnly")
                .collectionName("*")
                .dbName("*")
                .build());
    }

    public static void grantRole() {
        GrantRoleReq grantRoleReq = GrantRoleReq.builder()
                .roleName("role_a")
                .userName("user_1")
                .build();
        client.grantRole(grantRoleReq);
    }

    public static void describeUser() {
        DescribeUserReq describeUserReq = DescribeUserReq.builder()
                .userName("user_1")
                .build();
        DescribeUserResp describeUserResp = client.describeUser(describeUserReq);
        System.out.println(describeUserResp.getRoles());
    }

    public static void revokeUser() {
        client.revokeRole(RevokeRoleReq.builder()
                .userName("user_1")
                .roleName("role_a")
                .build());
    }

    public static void dropUser() {
        DropUserReq dropUserReq = DropUserReq.builder()
                .userName("user_1")
                .build();
        client.dropUser(dropUserReq);
    }

    public static void dropRole() {
        DropRoleReq dropRoleReq = DropRoleReq.builder()
                .roleName("role_a")
                .build();
        client.dropRole(dropRoleReq);
    }

    public static void main(String[] args) {
        createUser();
        updatePassword();
        listUsers();
        createRole();
        listRoles();
        createPrivilegeGroup();
        addPrivilegeToGroup();
        removePrivilegeFromGroup();
        listPrivilegeGroups();
        dropPrivilegeGroup();
        grantPrivilege();
        describeRole();
        revokePrivilege();
        grantRole();
        describeUser();
        revokeUser();
        dropUser();
        dropRole();
    }
}
