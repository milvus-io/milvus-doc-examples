package org.example.collection;

import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

public class DropCollection {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }

    public static void main(String[] args) {
        DropCollectionReq dropQuickSetupParam = DropCollectionReq.builder()
                .collectionName("customized_setup_2")
                .build();

        client.dropCollection(dropQuickSetupParam);
    }
}
