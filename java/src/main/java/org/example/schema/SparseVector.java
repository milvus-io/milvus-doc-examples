package org.example.schema;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.SparseFloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class SparseVector {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static void createCollection() {
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
                .fieldName("sparse_vector")
                .dataType(DataType.SparseFloatVector)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("drop_ratio_build", 0.2);
        indexes.add(IndexParam.builder()
                .fieldName("sparse_vector")
                .indexName("sparse_inverted_index")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(extraParams)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("my_sparse_collection")
                .collectionSchema(schema)
                .indexParams(indexes)
                .build();
        client.createCollection(requestCreate);
    }

    private static void insert() {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        {
            JsonObject row = new JsonObject();
            SortedMap<Long, Float> sparse = new TreeMap<>();
            sparse.put(1L, 0.5f);
            sparse.put(100L, 0.3f);
            sparse.put(500L, 0.8f);
            row.add("sparse_vector", gson.toJsonTree(sparse));
            rows.add(row);
        }
        {
            JsonObject row = new JsonObject();
            SortedMap<Long, Float> sparse = new TreeMap<>();
            sparse.put(10L, 0.1f);
            sparse.put(200L, 0.7f);
            sparse.put(1000L, 0.9f);
            row.add("sparse_vector", gson.toJsonTree(sparse));
            rows.add(row);
        }

        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName("my_sparse_collection")
                .data(rows)
                .build());
    }

    private static void search() {
        Map<String,Object> searchParams = new HashMap<>();
        searchParams.put("drop_ratio_search", 0.2);

        SortedMap<Long, Float> sparse = new TreeMap<>();
        sparse.put(10L, 0.1f);
        sparse.put(200L, 0.7f);
        sparse.put(1000L, 0.9f);

        SparseFloatVec queryVector = new SparseFloatVec(sparse);

        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName("my_sparse_collection")
                .data(Collections.singletonList(queryVector))
                .annsField("sparse_vector")
                .searchParams(searchParams)
                .topK(3)
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
