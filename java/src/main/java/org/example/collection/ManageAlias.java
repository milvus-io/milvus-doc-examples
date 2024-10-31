package org.example.collection;

import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

import io.milvus.v2.service.utility.request.ListAliasesReq;
import io.milvus.v2.service.utility.response.ListAliasResp;

import io.milvus.v2.service.utility.request.DescribeAliasReq;
import io.milvus.v2.service.utility.response.DescribeAliasResp;

import io.milvus.v2.service.utility.request.AlterAliasReq;

import io.milvus.v2.service.utility.request.DropAliasReq;

public class ManageAlias {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }

    private static void createAlias() {
        // 9. Manage aliases

        // 9.1 Create alias
        CreateAliasReq createAliasReq = CreateAliasReq.builder()
                .collectionName("customized_setup_2")
                .alias("bob")
                .build();

        client.createAlias(createAliasReq);

        createAliasReq = CreateAliasReq.builder()
                .collectionName("customized_setup_2")
                .alias("alice")
                .build();

        client.createAlias(createAliasReq);
    }

    private static void listAlias() {
        // 9.2 List alises
        ListAliasesReq listAliasesReq = ListAliasesReq.builder()
                .collectionName("customized_setup_2")
                .build();

        ListAliasResp listAliasRes = client.listAliases(listAliasesReq);

        System.out.println(listAliasRes.getAlias());
    }

    private static void descAlias() {
        DescribeAliasReq describeAliasReq = DescribeAliasReq.builder()
                .alias("bob")
                .build();

        DescribeAliasResp describeAliasRes = client.describeAlias(describeAliasReq);

        System.out.println(describeAliasRes);
    }

    private static void alterAlias() {
        // 9.4 Reassign alias to other collections
        AlterAliasReq alterAliasReq = AlterAliasReq.builder()
                .collectionName("customized_setup_1")
                .alias("alice")
                .build();

        client.alterAlias(alterAliasReq);

        ListAliasesReq listAliasesReq = ListAliasesReq.builder()
                .collectionName("customized_setup_1")
                .build();

        ListAliasResp listAliasRes = client.listAliases(listAliasesReq);

        System.out.println(listAliasRes.getAlias());

        listAliasesReq = ListAliasesReq.builder()
                .collectionName("customized_setup_2")
                .build();

        listAliasRes = client.listAliases(listAliasesReq);

        System.out.println(listAliasRes.getAlias());
    }

    private static void dropAlias() {
        // 9.5 Drop alias
        DropAliasReq dropAliasReq = DropAliasReq.builder()
                .alias("bob")
                .build();

        client.dropAlias(dropAliasReq);

        dropAliasReq = DropAliasReq.builder()
                .alias("alice")
                .build();

        client.dropAlias(dropAliasReq);
    }

    public static void main(String[] args) {
        createAlias();
        listAlias();
        descAlias();
        alterAlias();
        dropAlias();
    }
}
