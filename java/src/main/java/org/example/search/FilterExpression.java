package org.example.search;

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
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.*;

public class FilterExpression {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());
    }

    static private List<Float> genVector() {
        Random rnd = new Random();
        List<Float> vector = new ArrayList<>();
        for (int j = 0; j < 768; j++) {
            vector.add(rnd.nextFloat());
        }
        return vector;
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
                .dimension(768)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("color")
                .dataType(DataType.VarChar)
                .maxLength(100)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("price")
                .dataType(DataType.Float)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("inventory")
                .dataType(DataType.JSON)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("sales_volume")
                .dataType(DataType.Array)
                .elementType(DataType.Int32)
                .maxCapacity(100)
                .build());

        IndexParam indexParamForVectorField = IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build();

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(indexParamForVectorField);

        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName("my_collection")
                .collectionSchema(schema)
                .indexParams(indexParams)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build();
        client.createCollection(createCollectionReq);

        Gson gson = new Gson();
        List<JsonObject> data = Arrays.asList(
                gson.fromJson("{\"id\": 1, \"color\": \"pink_8682\", \"price\": 593, \"inventory\": {\"brand\": \"Sony\", \"quantity\": 310, \"previous_sales\": [451, 348, 224]}, \"sales_volume\": [161, 81, 51]}", JsonObject.class),
                gson.fromJson("{\"id\": 2, \"color\": \"red_7025\", \"price\": 196, \"inventory\": {\"brand\": \"Samsung\", \"quantity\": 274, \"previous_sales\": [315, 133, 109]}, \"sales_volume\": [126, 126, 125, 96, 155]}", JsonObject.class),
                gson.fromJson("{\"id\": 3, \"color\": \"orange_6781\", \"price\": 862, \"inventory\": {\"brand\": \"Samsung\", \"quantity\": 103, \"previous_sales\": [232, 254, 275]}, \"sales_volume\": [124, 117, 90, 188]}", JsonObject.class),
                gson.fromJson("{\"id\": 4, \"color\": \"pink_9298\", \"price\": 991, \"inventory\": {\"brand\": \"Microsoft\", \"quantity\": 175, \"previous_sales\": [288, 169, 112]}, \"sales_volume\": [133, 92, 181, 61, 193]}", JsonObject.class),
                gson.fromJson("{\"id\": 5, \"color\": \"red_4794\", \"price\": 327, \"inventory\": {\"brand\": \"Apple\", \"quantity\": 193, \"previous_sales\": [225, 286, 202]}, \"sales_volume\": [155, 161, 106, 86, 99]}", JsonObject.class),
                gson.fromJson("{\"id\": 6, \"color\": \"yellow_4222\", \"price\": 996, \"inventory\": {\"brand\": \"Microsoft\", \"quantity\": 376, \"previous_sales\": [254, 275, 232]}, \"sales_volume\": [173, 151, 148]}", JsonObject.class),
                gson.fromJson("{\"id\": 7, \"color\": \"red_9392\", \"price\": 848, \"inventory\": {\"brand\": \"Apple\", \"quantity\": 61, \"previous_sales\": [312, 254, 367]}, \"sales_volume\": [59, 156, 126, 60, 177]}", JsonObject.class),
                gson.fromJson("{\"id\": 8, \"color\": \"grey_8510\", \"price\": 241, \"inventory\": {\"brand\": \"Dell\", \"quantity\": 248, \"previous_sales\": [318, 238, 127]}, \"sales_volume\": [105, 126, 114, 132]}", JsonObject.class),
                gson.fromJson("{\"id\": 9, \"color\": \"white_9381\", \"price\": 597, \"inventory\": {\"brand\": \"Apple\", \"quantity\": 351, \"previous_sales\": [482, 105, 130]}, \"sales_volume\": [150, 150, 73]}", JsonObject.class),
                gson.fromJson("{\"id\": 10, \"color\": \"purple_4976\", \"price\": 450, \"inventory\": {\"brand\": \"Apple\", \"quantity\": 268, \"previous_sales\": [456, 271, 479]}, \"sales_volume\": [190, 149, 85, 79, 80]}", JsonObject.class)
        );

        for (JsonObject row : data) {
            row.add("vector", gson.toJsonTree(genVector()));
        }

        InsertReq insertReq = InsertReq.builder()
                .collectionName("my_collection")
                .data(data)
                .build();

        InsertResp insertResp = client.insert(insertReq);
    }

    private static void compareScalarQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("500 < price < 900")
                .outputFields(Arrays.asList("id", "color", "price"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void compareJsonQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("inventory[\"quantity\"] >= 250")
                .outputFields(Arrays.asList("id", "color", "price", "inventory"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void compareArrayQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("sales_volume[0] >= 150")
                .outputFields(Arrays.asList("id", "color", "price", "sales_volume"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void conditionScalarQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("color not in [\"red_7025\",\"red_4794\",\"red_9392\"]")
                .outputFields(Arrays.asList("id", "color", "price"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void conditionJsonQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("inventory[\"brand\"] in [\"Apple\"]")
                .outputFields(Arrays.asList("id", "color", "price", "inventory"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void matchScalarQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("color like \"red%\"")
                .outputFields(Arrays.asList("id", "color", "price"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void matchJsonQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("inventory[\"brand\"] like \"S%\"")
                .outputFields(Arrays.asList("id", "color", "price", "inventory"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }
    private static void mathScalarQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("200 <= price*0.5 and price*0.5 <= 300")
                .outputFields(Arrays.asList("id", "color", "price", "inventory"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void mathJsonQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("inventory[\"quantity\"] * 2 > 600")
                .outputFields(Arrays.asList("id", "color", "price", "inventory"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void mathArrayQuery() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("sales_volume[0]*2 > 300")
                .outputFields(Arrays.asList("id", "color", "price", "sales_volume"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void jsonContains() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("JSON_CONTAINS(inventory[\"previous_sales\"], 232)")
                .outputFields(Arrays.asList("id", "color", "price", "inventory"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void jsonContainsAll() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("JSON_CONTAINS_ALL(inventory[\"previous_sales\"], [232, 254, 275])")
                .outputFields(Arrays.asList("id", "color", "price", "inventory"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void jsonContainsAny() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("JSON_CONTAINS_ANY(inventory[\"previous_sales\"], [232, 254, 275])")
                .outputFields(Arrays.asList("id", "color", "price", "inventory"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void arrayContains() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("ARRAY_CONTAINS(sales_volume, 161)")
                .outputFields(Arrays.asList("id", "color", "price", "sales_volume"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void arrayContainsAll() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("ARRAY_CONTAINS_ALL(sales_volume, [150, 150])")
                .outputFields(Arrays.asList("id", "color", "price", "sales_volume"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void arrayContainsAny() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("ARRAY_CONTAINS_ANY(sales_volume, [150, 190, 90])")
                .outputFields(Arrays.asList("id", "color", "price", "sales_volume"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void arrayLength() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("ARRAY_LENGTH(sales_volume) == 3")
                .outputFields(Arrays.asList("id", "color", "price", "sales_volume"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void multiFilter() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("my_collection")
                .filter("color like \"red%\" and price < 500 and inventory[\"brand\"] in [\"Apple\"] and sales_volume[0] > 100")
                .outputFields(Arrays.asList("id", "color", "price", "inventory", "sales_volume"))
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }


    public static void main(String[] args) {
        createCollection();
        System.out.println("===== compareScalarQuery =====");
        compareScalarQuery();
        System.out.println("===== compareJsonQuery =====");
        compareJsonQuery();
        System.out.println("===== compareArrayQuery =====");
        compareArrayQuery();
        System.out.println("===== conditionScalarQuery =====");
        conditionScalarQuery();
        System.out.println("===== conditionJsonQuery =====");
        conditionJsonQuery();
        System.out.println("===== matchScalarQuery =====");
        matchScalarQuery();
        System.out.println("===== matchJsonQuery =====");
        matchJsonQuery();
        System.out.println("===== mathScalarQuery =====");
        mathScalarQuery();
        System.out.println("===== mathJsonQuery =====");
        mathJsonQuery();
        System.out.println("===== mathArrayQuery =====");
        mathArrayQuery();
        System.out.println("===== jsonContains =====");
        jsonContains();
        System.out.println("===== jsonContainsAll =====");
        jsonContainsAll();
        System.out.println("===== jsonContainsAny =====");
        jsonContainsAny();
        System.out.println("===== arrayContains =====");
        arrayContains();
        System.out.println("===== arrayContainsAll =====");
        arrayContainsAll();
        System.out.println("===== arrayContainsAny =====");
        arrayContainsAny();
        System.out.println("===== arrayLength =====");
        arrayLength();
        System.out.println("===== multiFilter =====");
        multiFilter();
    }
}
