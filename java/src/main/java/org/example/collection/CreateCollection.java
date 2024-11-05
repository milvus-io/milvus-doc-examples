package org.example.collection;

import io.milvus.param.Constant;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import io.milvus.v2.common.IndexParam;
import java.util.*;

import io.milvus.v2.service.collection.request.GetLoadStateReq;


public class CreateCollection {
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
        // 3. Create a collection in customized setup mode

        // 3.1 Create schema
        CreateCollectionReq.CollectionSchema schema = client.createSchema();

        // 3.2 Add fields to schema
        schema.addField(AddFieldReq.builder()
                .fieldName("my_id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("my_vector")
                .dataType(DataType.FloatVector)
                .dimension(5)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("my_varchar")
                .dataType(DataType.VarChar)
                .maxLength(512)
                .build());

        // 3.3 Prepare index parameters
        IndexParam indexParamForIdField = IndexParam.builder()
                .fieldName("my_id")
                .indexType(IndexParam.IndexType.STL_SORT)
                .build();

        IndexParam indexParamForVectorField = IndexParam.builder()
                .fieldName("my_vector")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build();

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(indexParamForIdField);
        indexParams.add(indexParamForVectorField);

        // 3.4 Create a collection with schema and index parameters
        CreateCollectionReq customizedSetupReq1 = CreateCollectionReq.builder()
                .collectionName("customized_setup_1")
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build();
        client.createCollection(customizedSetupReq1);

        // 3.5 Get load state of the collection
        GetLoadStateReq customSetupLoadStateReq1 = GetLoadStateReq.builder()
                .collectionName("customized_setup_1")
                .build();

        Boolean loaded = client.getLoadState(customSetupLoadStateReq1);
        System.out.println(loaded);

        // 3.6 Create a collection and index it separately
        CreateCollectionReq customizedSetupReq2 = CreateCollectionReq.builder()
                .collectionName("customized_setup_2")
                .collectionSchema(schema)
                .build();

        client.createCollection(customizedSetupReq2);

        GetLoadStateReq customSetupLoadStateReq2 = GetLoadStateReq.builder()
                .collectionName("customized_setup_2")
                .build();

        loaded = client.getLoadState(customSetupLoadStateReq2);
        System.out.println(loaded);

        // With shard number
        CreateCollectionReq customizedSetupReq3 = CreateCollectionReq.builder()
                .collectionName("customized_setup_3")
                .collectionSchema(schema)
                // highlight-next-line
                .numShards(1)
                .build();
        client.createCollection(customizedSetupReq3);

        // With MMap
        CreateCollectionReq customizedSetupReq4 = CreateCollectionReq.builder()
                .collectionName("customized_setup_4")
                .collectionSchema(schema)
                // highlight-next-line
                .property(Constant.MMAP_ENABLED, "false")
                .build();
        client.createCollection(customizedSetupReq4);

        // With TTL
        CreateCollectionReq customizedSetupReq5 = CreateCollectionReq.builder()
                .collectionName("customized_setup_5")
                .collectionSchema(schema)
                // highlight-next-line
                .property(Constant.TTL_SECONDS, "86400")
                .build();
        client.createCollection(customizedSetupReq5);

        // With consistency level
        CreateCollectionReq customizedSetupReq6 = CreateCollectionReq.builder()
                .collectionName("customized_setup_6")
                .collectionSchema(schema)
                // highlight-next-line
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(customizedSetupReq6);
    }
}
