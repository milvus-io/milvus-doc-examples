package org.example.schema;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

public class PrimaryKey {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static void Int64PrimaryKey() {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();

        schema.addField(AddFieldReq.builder()
                .fieldName("my_id")
                .dataType(DataType.Int64)
                // highlight-start
                .isPrimaryKey(true)
                .autoID(true)
                // highlight-end
                .build());
    }

    private static void VarcharPrimaryKey() {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();

        schema.addField(AddFieldReq.builder()
                .fieldName("my_id")
                .dataType(DataType.VarChar)
                // highlight-start
                .isPrimaryKey(true)
                .autoID(true)
                .maxLength(512)
                // highlight-end
                .build());
    }

    public static void main(String[] args) {
        Int64PrimaryKey();
        VarcharPrimaryKey();
    }
}
