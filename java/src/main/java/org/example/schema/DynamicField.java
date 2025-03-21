package org.example.schema;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class DynamicField {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());
    }

    private static void createCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("my_dynamic_collection")
                .build());
        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName("my_dynamic_collection")
                .dimension(5)
                // highlight-next-line
                .enableDynamicField(true)
                .build();
        client.createCollection(createCollectionReq);
    }

    private static void insert() {
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
                .collectionName("my_dynamic_collection")
                .data(data)
                .build();

        InsertResp insertResp = client.insert(insertReq);
        System.out.println(insertResp);
    }

    private static void createIndex() {
        List<IndexParam> indexes = new ArrayList<>();

        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("json_path", "color");
        extraParams.put("json_cast_type", "varchar");
        indexes.add(IndexParam.builder()
                .fieldName("color")
                .indexName("color_index")
                .indexType(IndexParam.IndexType.INVERTED)
                .extraParams(extraParams)
                .build());

        client.createIndex(CreateIndexReq.builder()
                .collectionName("my_dynamic_collection")
                .indexParams(indexes)
                .build());
    }

    private static void search() {
        FloatVec queryVector = new FloatVec(new float[]{0.3580376395471989f, -0.6023495712049978f, 0.18414012509913835f, -0.26286205330961354f, 0.9029438446296592f});
        SearchResp resp = client.search(SearchReq.builder()
                .collectionName("my_dynamic_collection")
                .annsField("vector")
                .data(Collections.singletonList(queryVector))
                .outputFields(Collections.singletonList("color"))
                .filter("color like \"red%\"")
                .topK(5)
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());

        System.out.println(resp.getSearchResults());
    }

    public static void main(String[] args) {
        createCollection();
        insert();
        createIndex();
        search();
    }
}
