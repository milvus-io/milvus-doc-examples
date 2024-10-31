package org.example.schema;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;

public class KnowSchema {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    public static void main(String[] args) {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();

        schema.addField(AddFieldReq.builder()
                .fieldName("my_id")
                .dataType(DataType.Int64)
                // highlight-start
                .isPrimaryKey(true)
                .autoID(false)
                // highlight-end
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("my_vector")
                .dataType(DataType.FloatVector)
                // highlight-next-line
                .dimension(5)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("my_varchar")
                .dataType(DataType.VarChar)
                // highlight-next-line
                .maxLength(512)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("my_int64")
                .dataType(DataType.Int64)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("my_bool")
                .dataType(DataType.Bool)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("my_json")
                .dataType(DataType.JSON)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("my_array")
                .dataType(DataType.Array)
                .elementType(DataType.VarChar)
                .maxCapacity(5)
                .maxLength(512)
                .build());

    }
}
