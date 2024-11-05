package org.example.schema;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class DenseVector {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static void createCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("my_dense_collection")
                .build());

        CreateCollectionReq.CollectionSchema schema = client.createSchema();
        schema.setEnableDynamicField(true);
        schema.addField(AddFieldReq.builder()
                .fieldName("pk")
                .dataType(DataType.VarChar)
                .isPrimaryKey(true)
                .autoID(true)
                .maxLength(100)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("dense_vector")
                .dataType(DataType.FloatVector)
                .dimension(4)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("nlist",128);
        indexes.add(IndexParam.builder()
                .fieldName("dense_vector")
                .indexName("dense_vector_index")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(extraParams)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("my_dense_collection")
                .collectionSchema(schema)
                .indexParams(indexes)
                .build();
        client.createCollection(requestCreate);
    }

    private static void insert() {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        rows.add(gson.fromJson("{\"dense_vector\": [0.1, 0.2, 0.3, 0.7]}", JsonObject.class));
        rows.add(gson.fromJson("{\"dense_vector\": [0.1, 0.2, 0.3, 0.7]}", JsonObject.class));

        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName("my_dense_collection")
                .data(rows)
                .build());
    }

    private static void search() {
        Map<String,Object> searchParams = new HashMap<>();
        searchParams.put("nprobe",10);

        FloatVec queryVector = new FloatVec(new float[]{0.1f, 0.3f, 0.3f, 0.7f});

        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName("my_dense_collection")
                .data(Collections.singletonList(queryVector))
                .annsField("dense_vector")
                .searchParams(searchParams)
                .topK(5)
                .outputFields(Collections.singletonList("pk"))
                .build());

        System.out.println(searchR.getSearchResults());
    }

    public static void main(String[] args) {
        createCollection();
        insert();
        search();
    }
}
