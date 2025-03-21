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

public class JsonField {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static CreateCollectionReq.CollectionSchema createSchema() {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();
        schema.setEnableDynamicField(true);

        schema.addField(AddFieldReq.builder()
                .fieldName("metadata")
                .dataType(DataType.JSON)
                .isNullable(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("pk")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("embedding")
                .dataType(DataType.FloatVector)
                .dimension(3)
                .build());

        return schema;
    }

    private static List<IndexParam> createIndex() {
        List<IndexParam> indexes = new ArrayList<>();

        Map<String,Object> extraParams_1 = new HashMap<>();
        extraParams_1.put("json_path", "metadata[\"product_info\"][\"category\"]");
        extraParams_1.put("json_cast_type", "varchar");
        indexes.add(IndexParam.builder()
                .fieldName("metadata")
                .indexName("json_index_1")
                .indexType(IndexParam.IndexType.INVERTED)
                .extraParams(extraParams_1)
                .build());

        Map<String,Object> extraParams_2 = new HashMap<>();
        extraParams_2.put("json_path", "metadata[\"price\"]");
        extraParams_2.put("json_cast_type", "double");
        indexes.add(IndexParam.builder()
                .fieldName("metadata")
                .indexName("json_index_2")
                .indexType(IndexParam.IndexType.INVERTED)
                .extraParams(extraParams_2)
                .build());

        indexes.add(IndexParam.builder()
                .fieldName("embedding")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        return indexes;
    }

    private static void createCollection(CreateCollectionReq.CollectionSchema schema, List<IndexParam> indexes) {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("my_json_collection")
                .build());
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("my_json_collection")
                .collectionSchema(schema)
                .indexParams(indexes)
                .build();
        client.createCollection(requestCreate);
    }

    private static void insert() {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        rows.add(gson.fromJson("{\"metadata\":{\"product_info\":{\"category\":\"electronics\",\"brand\":\"BrandA\"},\"price\":99.99,\"in_stock\":True,\"tags\":[\"summer_sale\"]},\"pk\":1,\"embedding\":[0.12,0.34,0.56]}", JsonObject.class));
        rows.add(gson.fromJson("{\"metadata\":null,\"pk\":2,\"embedding\":[0.56,0.78,0.90]}", JsonObject.class));
        rows.add(gson.fromJson("{\"pk\":3,\"embedding\":[0.91,0.18,0.23]}", JsonObject.class));
        rows.add(gson.fromJson("{\"metadata\":{\"product_info\":{\"category\":null,\"brand\":\"BrandB\"},\"price\":59.99,\"in_stock\":null},\"pk\":4,\"embedding\":[0.56,0.38,0.21]}", JsonObject.class));

        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName("my_json_collection")
                .data(rows)
                .build());
    }

    private static void query(String filter) {
        System.out.println(String.format("======= Query with filter: '%s' =======", filter));
        QueryResp resp = client.query(QueryReq.builder()
                .collectionName("my_json_collection")
                .filter(filter)
                .outputFields(Arrays.asList("metadata", "pk"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        System.out.println(resp.getQueryResults());
    }

    private static void search(String filter) {
        System.out.println(String.format("======= Search with filter: '%s' =======", filter));
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName("my_json_collection")
                .annsField("embedding")
                .data(Collections.singletonList(new FloatVec(new float[]{0.3f, -0.6f, 0.1f})))
                .topK(5)
                .outputFields(Collections.singletonList("metadata"))
                .filter(filter)
                .build());

        System.out.println(resp.getSearchResults());
    }

    public static void main(String[] args) {
        CreateCollectionReq.CollectionSchema schema = createSchema();
        List<IndexParam> indexes = createIndex();
        createCollection(schema, indexes);
        insert();
        query("metadata is not null");
        query("metadata[\"product_info\"][\"category\"] == \"electronics\"");
        search("metadata[\"product_info\"][\"brand\"] == \"BrandA\"");
    }
}
