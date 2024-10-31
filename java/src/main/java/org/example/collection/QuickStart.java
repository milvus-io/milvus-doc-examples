package org.example.collection;

import com.google.gson.*;
import io.milvus.v2.client.*;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.vector.request.*;
import io.milvus.v2.service.vector.request.data.*;
import io.milvus.v2.service.vector.response.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

    private static JsonObject createRow(long id, List<Float> vector, String color) {
        JsonObject row = new JsonObject();
        row.addProperty("id", id);
        row.add("vector", new Gson().toJsonTree(vector));
        row.addProperty("color", color);
        return row;
    }
    public static void main(String[] args) {
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

        // Insert some data
        List<JsonObject> rows = Arrays.asList(
                createRow(0L, Arrays.asList(0.3580376395471989f, -0.6023495712049978f, 0.18414012509913835f, -0.26286205330961354f, 0.9029438446296592f), "pink_8682"),
                createRow(1L, Arrays.asList(0.19886812562848388f, 0.06023560599112088f, 0.6976963061752597f, 0.2614474506242501f, 0.838729485096104f), "red_7025"),
                createRow(2L, Arrays.asList(0.43742130801983836f, -0.5597502546264526f, 0.6457887650909682f, 0.7894058910881185f, 0.20785793220625592f), "orange_6781"),
                createRow(3L, Arrays.asList(0.3172005263489739f, 0.9719044792798428f, -0.36981146090600725f, -0.4860894583077995f, 0.95791889146345f), "pink_9298"),
                createRow(4L, Arrays.asList(0.4452349528804562f, -0.8757026943054742f, 0.8220779437047674f, 0.46406290649483184f, 0.30337481143159106f), "red_4794"),
                createRow(5L, Arrays.asList(0.985825131989184f, -0.8144651566660419f, 0.6299267002202009f, 0.1206906911183383f, -0.1446277761879955f), "yellow_4222"),
                createRow(6L, Arrays.asList(0.8371977790571115f, -0.015764369584852833f, -0.31062937026679327f, -0.562666951622192f, -0.8984947637863987f), "red_9392"),
                createRow(7L, Arrays.asList(-0.33445148015177995f, -0.2567135004164067f, 0.8987539745369246f, 0.9402995886420709f, 0.5378064918413052f), "grey_8510"),
                createRow(8L, Arrays.asList(0.39524717779832685f, 0.4000257286739164f, -0.5890507376891594f, -0.8650502298996872f, -0.6140360785406336f), "white_9381"),
                createRow(9L, Arrays.asList(0.5718280481994695f, 0.24070317428066512f, -0.3737913482606834f, -0.06726932177492717f, -0.6980531615588608f), "purple_4976")
        );

        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName("quick_setup")
                .data(rows)
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


        // Search
        List<BaseVector> queryVectors = Collections.singletonList(
                new FloatVec(Arrays.asList(0.041732933f, 0.013779674f, -0.027564144f, -0.013061441f, 0.009748648f))
        );

        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName("quick_setup")
                .data(queryVectors)
                .filter("$meta[\"color\"] like \"red%\"")
                .topK(3)
                .outputFields(Collections.singletonList("color"))
                .build());

        System.out.println(searchR.getSearchResults());

        QueryReq queryReq = QueryReq.builder()
                .collectionName("quick_setup")
                .filter("$meta[\"color\"] like \"pink_%\"")
                .outputFields(Collections.singletonList("color"))
                .limit(5)
                .build();

        QueryResp queryRes = client.query(queryReq);
        System.out.println(queryRes.getQueryResults());

        GetReq getReq = GetReq.builder()
                .collectionName("quick_setup")
                .ids(Arrays.asList(0L, 1L, 2L))
                .build();

        GetResp getRes = client.get(getReq);
        System.out.println(getRes.getGetResults());

        DeleteReq deleteReq = DeleteReq.builder()
                .collectionName("quick_setup")
                .ids(Arrays.asList(0L, 1L, 2L, 3L, 4L))
                .build();

        DeleteResp deleteRes = client.delete(deleteReq);
        System.out.println(deleteRes);

        DeleteReq filterDeleteReq = DeleteReq.builder()
                .collectionName("quick_setup")
                .filter("id in [5, 6, 7, 8, 9]")
                .build();

        DeleteResp filterDeleteRes = client.delete(filterDeleteReq);
        System.out.println(filterDeleteRes);

        client.close();
    }
}