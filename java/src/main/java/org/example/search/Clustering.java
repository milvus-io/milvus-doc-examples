package org.example.search;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.CompactionState;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.utility.request.CompactReq;
import io.milvus.v2.service.utility.request.GetCompactionStateReq;
import io.milvus.v2.service.utility.response.CompactResp;
import io.milvus.v2.service.utility.response.GetCompactionStateResp;

import java.util.concurrent.TimeUnit;


public class Clustering {
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
                .isClusteringKey(true)
                .build());


        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName("my_collection")
                .collectionSchema(schema)
                .build();
        client.createCollection(createCollectionReq);
    }

    public static void main(String[] args) throws InterruptedException {
        createCollection();

        CompactResp compactResp = client.compact(CompactReq.builder()
                .collectionName("my_collection")
                .isClustering(true)
                .build());

        CompactionState state = CompactionState.UndefiedState;
        while (state != CompactionState.Completed) {
            TimeUnit.SECONDS.sleep(1);
            GetCompactionStateResp getResp = client.getCompactionState(GetCompactionStateReq.builder()
                    .compactionID(compactResp.getCompactionID())
                    .build());
            state = getResp.getState();
        }

    }
}
