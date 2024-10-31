package org.example.search;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.vector.request.GetReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.response.GetResp;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.QueryResp;

import java.util.*;

public class Query {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());
    }

    static private void createCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("query_collection")
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
                .build());

        IndexParam indexParamForVectorField = IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build();

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(indexParamForVectorField);

        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName("query_collection")
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build();
        client.createCollection(createCollectionReq);

        Gson gson = new Gson();
        List<JsonObject> data = Arrays.asList(
                gson.fromJson("{\"id\": 0, \"vector\": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592], \"color\": \"pink_8682\"}", JsonObject.class),
                gson.fromJson("{\"id\": 1, \"vector\": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104], \"color\": \"red_7025\"}", JsonObject.class),
                gson.fromJson("{\"id\": 2, \"vector\": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592], \"color\": \"orange_6781\"}", JsonObject.class),
                gson.fromJson("{\"id\": 3, \"vector\": [0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995, 0.95791889146345], \"color\": \"pink_9298\"}", JsonObject.class),
                gson.fromJson("{\"id\": 4, \"vector\": [0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184, 0.30337481143159106], \"color\": \"red_4794\"}", JsonObject.class),
                gson.fromJson("{\"id\": 5, \"vector\": [0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955], \"color\": \"yellow_4222\"}", JsonObject.class),
                gson.fromJson("{\"id\": 6, \"vector\": [0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192, -0.8984947637863987], \"color\": \"red_9392\"}", JsonObject.class),
                gson.fromJson("{\"id\": 7, \"vector\": [-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052], \"color\": \"grey_8510\"}", JsonObject.class),
                gson.fromJson("{\"id\": 8, \"vector\": [0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872, -0.6140360785406336], \"color\": \"white_9381\"}", JsonObject.class),
                gson.fromJson("{\"id\": 9, \"vector\": [0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717, -0.6980531615588608], \"color\": \"purple_4976\"}", JsonObject.class)
        );

        InsertReq insertReq = InsertReq.builder()
                .collectionName("query_collection")
                .data(data)
                .build();

        InsertResp insertResp = client.insert(insertReq);

        data = Arrays.asList(
                gson.fromJson("{\"id\": 10, \"vector\": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592], \"color\": \"pink_8682\"}", JsonObject.class),
                gson.fromJson("{\"id\": 11, \"vector\": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104], \"color\": \"red_7025\"}", JsonObject.class),
                gson.fromJson("{\"id\": 12, \"vector\": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592], \"color\": \"orange_6781\"}", JsonObject.class),
                gson.fromJson("{\"id\": 13, \"vector\": [0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995, 0.95791889146345], \"color\": \"pink_9298\"}", JsonObject.class),
                gson.fromJson("{\"id\": 14, \"vector\": [0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184, 0.30337481143159106], \"color\": \"red_4794\"}", JsonObject.class),
                gson.fromJson("{\"id\": 15, \"vector\": [0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955], \"color\": \"yellow_4222\"}", JsonObject.class),
                gson.fromJson("{\"id\": 16, \"vector\": [0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192, -0.8984947637863987], \"color\": \"red_9392\"}", JsonObject.class),
                gson.fromJson("{\"id\": 17, \"vector\": [-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052], \"color\": \"grey_8510\"}", JsonObject.class),
                gson.fromJson("{\"id\": 18, \"vector\": [0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872, -0.6140360785406336], \"color\": \"white_9381\"}", JsonObject.class),
                gson.fromJson("{\"id\": 19, \"vector\": [0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717, -0.6980531615588608], \"color\": \"purple_4976\"}", JsonObject.class)
        );

        client.createPartition(CreatePartitionReq.builder()
                .collectionName("query_collection")
                .partitionName("partitionA")
                .build());

        client.insert(InsertReq.builder()
                .collectionName("query_collection")
                .partitionName("partitionA")
                .data(data)
                .build());
    }

    private static void getByID() {
        GetReq getReq = GetReq.builder()
                .collectionName("query_collection")
                .ids(Arrays.asList(0, 1, 2))
                .outputFields(Arrays.asList("vector", "color"))
                .build();

        GetResp getResp = client.get(getReq);

        List<QueryResp.QueryResult> results = getResp.getGetResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }
    private static void queryByFilter() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("query_collection")
                .filter("color like \"red%\"")
                .outputFields(Arrays.asList("vector", "color"))
                .limit(3)
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void queryByIterator() {
        QueryIteratorReq req = QueryIteratorReq.builder()
                .collectionName("query_collection")
                .expr("color like \"red%\"")
                .batchSize(50L)
                .outputFields(Collections.singletonList("color"))
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        QueryIterator queryIterator = client.queryIterator(req);

        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                System.out.println(record);
            }
        }
    }

    private static void getInPartition() {
        GetReq getReq = GetReq.builder()
                .collectionName("query_collection")
                .partitionName("partitionA")
                .ids(Arrays.asList(10, 11, 12))
                .outputFields(Collections.singletonList("color"))
                .build();

        GetResp getResp = client.get(getReq);

        List<QueryResp.QueryResult> results = getResp.getGetResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void queryInPartition() {
        QueryReq queryReq = QueryReq.builder()
                .collectionName("query_collection")
                .partitionNames(Collections.singletonList("partitionA"))
                .filter("color like \"red%\"")
                .outputFields(Collections.singletonList("color"))
                .limit(3)
                .build();

        QueryResp getResp = client.query(queryReq);

        List<QueryResp.QueryResult> results = getResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }
    }

    private static void queryIteratorInPartition() {
        QueryIteratorReq req = QueryIteratorReq.builder()
                .collectionName("query_collection")
                .partitionNames(Collections.singletonList("partitionA"))
                .expr("color like \"red%\"")
                .batchSize(50L)
                .outputFields(Collections.singletonList("color"))
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        QueryIterator queryIterator = client.queryIterator(req);

        while (true) {
            List<QueryResultsWrapper.RowRecord> res = queryIterator.next();
            if (res.isEmpty()) {
                queryIterator.close();
                break;
            }

            for (QueryResultsWrapper.RowRecord record : res) {
                System.out.println(record);
            }
        }
    }

    public static void main(String[] args) {
        createCollection();
        System.out.println("===== getByID =====");
        getByID();
        System.out.println("===== queryByFilter =====");
        queryByFilter();
        System.out.println("===== queryByIterator =====");
        queryByIterator();
        System.out.println("===== getInPartition =====");
        getInPartition();
        System.out.println("===== queryInPartition =====");
        queryInPartition();
        System.out.println("===== queryIteratorInPartition =====");
        queryIteratorInPartition();
    }
}
