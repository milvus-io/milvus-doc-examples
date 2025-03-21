package org.example.storage;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.utility.request.CompactReq;
import io.milvus.v2.service.utility.request.GetCompactionStateReq;
import io.milvus.v2.service.utility.response.CompactResp;
import io.milvus.v2.service.utility.response.GetCompactionStateResp;

public class Clustering {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());
    }

    public static void main(String[] args) {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();

        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("key")
                .dataType(DataType.Int64)
                .isClusteringKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("var")
                .dataType(DataType.VarChar)
                .maxLength(1000)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(5)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName("clustering_test")
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("clustering_test")
                .collectionSchema(schema)
                .build();
        client.createCollection(requestCreate);

        CompactResp compactResp = client.compact(CompactReq.builder()
                .collectionName("clustering_test")
                .isClustering(true)
                .build());

        GetCompactionStateResp stateResp = client.getCompactionState(GetCompactionStateReq.builder()
                .compactionID(compactResp.getCompactionID())
                .build());

        System.out.println(stateResp.getState());
    }
}
