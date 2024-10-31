package org.example.collection;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.collection.response.ListCollectionsResp;

public class DescribeCollection {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }
    private static void listCollections() {
        ListCollectionsResp resp = client.listCollections();
        System.out.println(resp.getCollectionNames());
    }

    private static void describeCollection() {
        DescribeCollectionReq request = DescribeCollectionReq.builder()
                .collectionName("quick_setup")
                .build();
        DescribeCollectionResp resp = client.describeCollection(request);
        System.out.println(resp);
    }

    public static void main(String[] args) {
        listCollections();
        describeCollection();
    }
}
