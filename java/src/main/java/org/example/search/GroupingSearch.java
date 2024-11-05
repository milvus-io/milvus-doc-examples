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
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class GroupingSearch {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());
    }

    static private void createCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("group_search_collection")
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
                .fieldName("chunk")
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("docId")
                .dataType(DataType.Int64)
                .build());

        IndexParam indexParamForVectorField = IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build();

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(indexParamForVectorField);

        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName("group_search_collection")
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build();
        client.createCollection(createCollectionReq);

        Gson gson = new Gson();
        List<JsonObject> data = Arrays.asList(
                gson.fromJson("{\"id\": 0, \"vector\": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592], \"chunk\": \"pink_8682\", \"docId\": 1}", JsonObject.class),
                gson.fromJson("{\"id\": 1, \"vector\": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104], \"chunk\": \"red_7025\", \"docId\": 5}", JsonObject.class),
                gson.fromJson("{\"id\": 2, \"vector\": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592], \"chunk\": \"orange_6781\", \"docId\": 2}", JsonObject.class),
                gson.fromJson("{\"id\": 3, \"vector\": [0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995, 0.95791889146345], \"chunk\": \"pink_9298\", \"docId\": 3}", JsonObject.class),
                gson.fromJson("{\"id\": 4, \"vector\": [0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184, 0.30337481143159106], \"chunk\": \"red_4794\", \"docId\": 3}", JsonObject.class),
                gson.fromJson("{\"id\": 5, \"vector\": [0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955], \"chunk\": \"yellow_4222\", \"docId\": 4}", JsonObject.class),
                gson.fromJson("{\"id\": 6, \"vector\": [0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192, -0.8984947637863987], \"chunk\": \"red_9392\", \"docId\": 1}", JsonObject.class),
                gson.fromJson("{\"id\": 7, \"vector\": [-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052], \"chunk\": \"grey_8510\", \"docId\": 2}", JsonObject.class),
                gson.fromJson("{\"id\": 8, \"vector\": [0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872, -0.6140360785406336], \"chunk\": \"white_9381\", \"docId\": 5}", JsonObject.class),
                gson.fromJson("{\"id\": 9, \"vector\": [0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717, -0.6980531615588608], \"chunk\": \"purple_4976\", \"docId\": 3}", JsonObject.class)
        );

        InsertReq insertReq = InsertReq.builder()
                .collectionName("group_search_collection")
                .data(data)
                .build();

        InsertResp insertResp = client.insert(insertReq);
    }

    private static void groupSearch() {
        FloatVec queryVector = new FloatVec(new float[]{0.14529211512077012f, 0.9147257273453546f, 0.7965055218724449f, 0.7009258593102812f, 0.5605206522382088f});
        SearchReq searchReq = SearchReq.builder()
                .collectionName("group_search_collection")
                .data(Collections.singletonList(queryVector))
                .topK(3)
                .groupByFieldName("docId")
                .outputFields(Collections.singletonList("docId"))
                .build();

        SearchResp searchResp = client.search(searchReq);

        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            System.out.println("TopK results:");
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
    }

    private static void groupSizeSearch() {
        FloatVec queryVector = new FloatVec(new float[]{0.14529211512077012f, 0.9147257273453546f, 0.7965055218724449f, 0.7009258593102812f, 0.5605206522382088f});
        SearchReq searchReq = SearchReq.builder()
                .collectionName("group_search_collection")
                .data(Collections.singletonList(queryVector))
                .topK(5)
                .groupByFieldName("docId")
                .groupSize(2)
                .groupStrictSize(true)
                .outputFields(Collections.singletonList("docId"))
                .build();

        SearchResp searchResp = client.search(searchReq);

        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            System.out.println("TopK results:");
            for (SearchResp.SearchResult result : results) {
                System.out.println(result);
            }
        }
    }

    public static void main(String[] args) {
        createCollection();
        System.out.println("===== groupSearch =====");
        groupSearch();
        System.out.println("===== groupSizeSearch =====");
        groupSizeSearch();
    }
}
