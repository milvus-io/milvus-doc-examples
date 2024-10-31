package org.example.search;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.AlterCollectionReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;

import java.util.*;

public class Mmap {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());
    }

    static private void createCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("my_collection")
                .build());

        CreateCollectionReq.CollectionSchema schema = client.createSchema();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(5)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("color")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());


        Map<String, String> properties = new HashMap<>();
        properties.put("mmap.enabled", "false");
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName("my_collection")
                .collectionSchema(schema)
                .properties(properties)
                .build();
        client.createCollection(createCollectionReq);

        properties.put("mmap.enabled", "true");
        client.alterCollection(AlterCollectionReq.builder()
                .collectionName("my_collection")
                .properties(properties)
                .build());
    }

    public static void main(String[] args) {
        createCollection();
    }
}
