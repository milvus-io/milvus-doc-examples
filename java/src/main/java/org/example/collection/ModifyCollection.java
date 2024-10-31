package org.example.collection;

import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.RenameCollectionReq;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

import io.milvus.v2.service.collection.request.AlterCollectionReq;
import java.util.HashMap;
import java.util.Map;

public class ModifyCollection {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }


    private static void rename() {
        CreateCollectionReq quickSetupReq = CreateCollectionReq.builder()
                .collectionName("my_collection")
                .dimension(5)
                .build();

        client.createCollection(quickSetupReq);

        RenameCollectionReq renameCollectionReq = RenameCollectionReq.builder()
                .collectionName("my_collection")
                .newCollectionName("my_new_collection")
                .build();

        client.renameCollection(renameCollectionReq);
    }

    private static void alter() {
        CreateCollectionReq quickSetupReq = CreateCollectionReq.builder()
                .collectionName("my_collection")
                .dimension(5)
                .build();

        client.createCollection(quickSetupReq);

        Map<String, String> properties = new HashMap<>();
        properties.put("collection.ttl.seconds", "60");

        AlterCollectionReq alterCollectionReq = AlterCollectionReq.builder()
                .collectionName("my_collection")
                .properties(properties)
                .build();

        client.alterCollection(alterCollectionReq);
    }

    public static void main(String[] args) {
        rename();
        alter();
    }
}
