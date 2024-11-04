package org.example;

import com.google.gson.*;
import io.milvus.v2.client.*;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.*;

import java.util.*;

public class QuickStart {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }

    private static void createCollection() {
        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("quick_setup")
                .build());

        // Quickly create a collection with "id" field and "vector" field
        client.createCollection(CreateCollectionReq.builder()
                .collectionName("quick_setup")
                .dimension(5)
                .build());
        System.out.printf("Collection '%s' created\n", "quick_setup");

        // 4. Insert data into the collection
        // 4.1. Prepare data
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

        // 4.2. Insert data
        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName("quick_setup")
                .data(data)
                .build());
        System.out.println(insertR);
        System.out.printf("%d rows inserted\n", insertR.getInsertCnt());

        // Get row count, set ConsistencyLevel.STRONG to sync the data to query node so that data is visible
        QueryResp countR = client.query(QueryReq.builder()
                .collectionName("quick_setup")
                .filter("")
                .outputFields(Collections.singletonList("count(*)"))
                .consistencyLevel(ConsistencyLevel.STRONG)
                .build());
        System.out.printf("%d rows persisted\n", (long)countR.getQueryResults().get(0).getEntity().get("count(*)"));
    }

    private static void searchSingleVector() {
        // 5. Search with a single vector
        // 5.1. Prepare query vectors
        List<BaseVector> queryVectors = Collections.singletonList(
                new FloatVec(Arrays.asList(0.041732933f, 0.013779674f, -0.027564144f, -0.013061441f, 0.009748648f))
        );

        // 5.2. Start search
        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName("quick_setup")
                .data(queryVectors)
                .topK(3)
                .build());

        System.out.println(searchR.getSearchResults());
    }

    private static void searchBatchVectors() {
        // 6. Search with multiple vectors
        // 6.1. Prepare query vectors
        List<BaseVector> queryVectors = Arrays.asList(
                new FloatVec(Arrays.asList(0.041732933f, 0.013779674f, -0.027564144f, -0.013061441f, 0.009748648f)),
                new FloatVec(Arrays.asList(0.0039737443f, 0.003020432f, -0.0006188639f, 0.03913546f, -0.00089768134f))
        );

        // 6.2. Start search
        SearchResp multiVectorSearchRes = client.search(SearchReq.builder()
                .collectionName("quick_setup")
                .data(queryVectors)
                .topK(3)
                .build());

        System.out.println(multiVectorSearchRes.getSearchResults());
    }

    private static void filteredSearchDefinedField() {
        // 7. Search with a filter expression using schema-defined fields
        // 7.1 Prepare query vectors
        List<BaseVector> queryVectors = Collections.singletonList(
                new FloatVec(Arrays.asList(0.041732933f, 0.013779674f, -0.027564144f, -0.013061441f, 0.009748648f))
        );

        // 7.2. Start search
        SearchResp filteredVectorSearchRes = client.search(SearchReq.builder()
                .collectionName("quick_setup")
                .data(queryVectors)
                .filter("1 < id < 8")
                .topK(3)
                .build());
        System.out.println(filteredVectorSearchRes.getSearchResults());
    }

    private static void filteredSearchDynamicField() {
        // 8. Search with a filter expression using custom fields
        // 8.1.Prepare query vectors
        List<BaseVector> queryVectors = Collections.singletonList(
                new FloatVec(Arrays.asList(0.041732933f, 0.013779674f, -0.027564144f, -0.013061441f, 0.009748648f))
        );

        // 8.2.Start search
        SearchResp customFilteredVectorSearchRes = client.search(SearchReq.builder()
                .collectionName("quick_setup")
                .data(queryVectors)
                .filter("$meta[\"color\"] like \"red%\"")
                .topK(3)
                .outputFields(Collections.singletonList("color"))
                .build());
        System.out.println(customFilteredVectorSearchRes.getSearchResults());
    }

    private static void queryDefinedField() {
        // 9. Query with filter using schema-defined fields
        QueryReq queryReq = QueryReq.builder()
                .collectionName("quick_setup")
                .filter("1 < id < 5")
                .outputFields(Collections.singletonList("color"))
                .limit(5)
                .build();

        QueryResp queryRes = client.query(queryReq);
        System.out.println(queryRes.getQueryResults());
    }

    private static void queryDynamicField() {
        // 10. Query with filter using custom fields
        QueryReq queryReq = QueryReq.builder()
                .collectionName("quick_setup")
                .filter("$meta[\"color\"] like \"pink_8%\"")
                .outputFields(Collections.singletonList("color"))
                .limit(5)
                .build();

        QueryResp queryRes = client.query(queryReq);
        System.out.println(queryRes.getQueryResults());
    }
    private static void get() {
        // 11. Get entities by IDs
        GetReq getReq = GetReq.builder()
                .collectionName("quick_setup")
                .ids(Arrays.asList(1L, 2L, 3L))
                .build();

        GetResp getRes = client.get(getReq);
        System.out.println(getRes.getGetResults());
    }

    private static void deleteByID() {
        // 12. Delete entities by IDs
        DeleteReq deleteReq = DeleteReq.builder()
                .collectionName("quick_setup")
                .ids(Arrays.asList(0L, 1L, 2L, 3L, 4L))
                .build();

        DeleteResp deleteRes = client.delete(deleteReq);
        System.out.println(deleteRes);
    }

    private static void deleteByExpr() {
        // 13. Delete entities by filter
        DeleteReq filterDeleteReq = DeleteReq.builder()
                .collectionName("quick_setup")
                .filter("id in [5, 6, 7, 8, 9]")
                .build();

        DeleteResp filterDeleteRes = client.delete(filterDeleteReq);
        System.out.println(filterDeleteRes);
    }

    private static void dropCollection() {
        // 14. Drop collections
        DropCollectionReq dropQuickSetupReq = DropCollectionReq.builder()
                .collectionName("quick_setup")
                .build();

        client.dropCollection(dropQuickSetupReq);
    }


    public static void main(String[] args) {
        createCollection();
        System.out.println("===== searchSingleVector =====");
        searchSingleVector();
        System.out.println("===== searchBatchVectors =====");
        searchBatchVectors();
        System.out.println("===== filteredSearchDefinedField =====");
        filteredSearchDefinedField();
        System.out.println("===== filteredSearchDynamicField =====");
        filteredSearchDynamicField();
        System.out.println("===== queryDefinedField =====");
        queryDefinedField();
        System.out.println("===== queryDynamicField =====");
        queryDynamicField();
        System.out.println("===== get =====");
        get();
        System.out.println("===== deleteByID =====");
        deleteByID();
        System.out.println("===== deleteByExpr =====");
        deleteByExpr();

        dropCollection();
        client.close();
    }
}