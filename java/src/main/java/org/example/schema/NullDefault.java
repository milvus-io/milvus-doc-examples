package org.example.schema;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class NullDefault {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static void dropCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("user_profiles_null")
                .build());
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("user_profiles_default")
                .build());
    }

    private static void createCollectionForNull() {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();
        schema.setEnableDynamicField(true);

        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(5)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("age")
                .dataType(DataType.Int64)
                .isNullable(true)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 128);
        indexes.add(IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(extraParams)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("user_profiles_null")
                .collectionSchema(schema)
                .indexParams(indexes)
                .build();
        client.createCollection(requestCreate);
    }

    private static void insertNull() {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        rows.add(gson.fromJson("{\"id\": 1, \"vector\": [0.1, 0.2, 0.3, 0.4, 0.5], \"age\": 30}", JsonObject.class));
        rows.add(gson.fromJson("{\"id\": 2, \"vector\": [0.2, 0.3, 0.4, 0.5, 0.6], \"age\": null}", JsonObject.class));
        rows.add(gson.fromJson("{\"id\": 3, \"vector\": [0.3, 0.4, 0.5, 0.6, 0.7]}", JsonObject.class));

        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName("user_profiles_null")
                .data(rows)
                .build());
    }

    private static void searchNull() {
        Map<String,Object> params = new HashMap<>();
        params.put("nprobe", 16);
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName("user_profiles_null")
                .annsField("vector")
                .data(Collections.singletonList(new FloatVec(new float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f})))
                .topK(2)
                .searchParams(params)
                .outputFields(Arrays.asList("id", "age"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        System.out.println(resp.getSearchResults());
    }

    private static void queryWithoutNull() {
        QueryResp resp = client.query(QueryReq.builder()
                .collectionName("user_profiles_null")
                .filter("age >= 0")
                .outputFields(Arrays.asList("id", "age"))
                .build());

        System.out.println(resp.getQueryResults());
    }

    private static void queryIncludeNull() {
        QueryResp resp = client.query(QueryReq.builder()
                .collectionName("user_profiles_null")
                .filter("")
                .outputFields(Arrays.asList("id", "age"))
                .limit(10)
                .build());

        System.out.println(resp.getQueryResults());
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////
    private static void createCollectionForDefault() {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();
        schema.setEnableDynamicField(true);

        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(5)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("age")
                .dataType(DataType.Int64)
                .defaultValue(18L)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("status")
                .dataType(DataType.VarChar)
                .maxLength(10)
                .defaultValue("active")
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("nlist", 128);
        indexes.add(IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.L2)
                .extraParams(extraParams)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("user_profiles_default")
                .collectionSchema(schema)
                .indexParams(indexes)
                .build();
        client.createCollection(requestCreate);
    }

    private static void insertDefault() {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        rows.add(gson.fromJson("{\"id\": 1, \"vector\": [0.1, 0.2, 0.3, 0.4, 0.5], \"age\": 30, \"status\": \"premium\"}", JsonObject.class));
        rows.add(gson.fromJson("{\"id\": 2, \"vector\": [0.2, 0.3, 0.4, 0.5, 0.6]}", JsonObject.class));
        rows.add(gson.fromJson("{\"id\": 3, \"vector\": [0.3, 0.4, 0.5, 0.6, 0.7], \"age\": 25, \"status\": null}", JsonObject.class));
        rows.add(gson.fromJson("{\"id\": 4, \"vector\": [0.4, 0.5, 0.6, 0.7, 0.8], \"age\": null, \"status\": \"inactive\"}", JsonObject.class));

        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName("user_profiles_default")
                .data(rows)
                .build());
    }

    private static void searchDefault() {
        Map<String,Object> params = new HashMap<>();
        params.put("nprobe", 16);
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName("user_profiles_default")
                .annsField("vector")
                .data(Collections.singletonList(new FloatVec(new float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f})))
                .searchParams(params)
                .filter("age == 18")
                .topK(10)
                .outputFields(Arrays.asList("id", "age", "status"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        System.out.println(resp.getSearchResults());
    }

    private static void queryDefault() {
        QueryResp ageResp = client.query(QueryReq.builder()
                .collectionName("user_profiles_default")
                .filter("age == 18")
                .outputFields(Arrays.asList("id", "age", "status"))
                .build());

        System.out.println(ageResp.getQueryResults());

        QueryResp statusResp = client.query(QueryReq.builder()
                .collectionName("user_profiles_default")
                .filter("status == \"active\"")
                .outputFields(Arrays.asList("id", "age", "status"))
                .build());

        System.out.println(statusResp.getQueryResults());
    }

    public static void main(String[] args) {
        dropCollection();
        createCollectionForNull();
        insertNull();
        searchNull();
        queryWithoutNull();
        queryIncludeNull();

        dropCollection();
        createCollectionForDefault();
        insertDefault();
        searchDefault();
        queryDefault();
    }
}
