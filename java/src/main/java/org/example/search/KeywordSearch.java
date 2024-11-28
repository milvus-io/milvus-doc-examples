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
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.response.QueryResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class KeywordSearch {
    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("keyword_search_collection")
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .enableDynamicField(false)
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(true)
                .build());
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("type", "english");
        schema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(1000)
                .enableAnalyzer(true)
                .analyzerParams(analyzerParams)
                .enableMatch(true) // must enable this if you use TextMatch
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("embeddings")
                .dataType(DataType.FloatVector)
                .dimension(5)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName("embeddings")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("keyword_search_collection")
                .collectionSchema(schema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // Insert rows
        Gson gson = new Gson();
        List<JsonObject> rows = Arrays.asList(
                gson.fromJson("{\"text\": \"Artificial intelligence was founded as an academic discipline in 1956.\", \"embeddings\": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592]}", JsonObject.class),
                gson.fromJson("{\"text\": \"Alan Turing was the first person to conduct substantial research in AI.\", \"embeddings\": [0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955]}", JsonObject.class),
                gson.fromJson("{\"text\": \"Born in Maida Vale, London, Turing was raised in southern England.\", \"embeddings\": [-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052]}", JsonObject.class)
        );

        client.insert(InsertReq.builder()
                .collectionName("keyword_search_collection")
                .data(rows)
                .build());

        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName("keyword_search_collection")
                .data(Collections.singletonList(new FloatVec(new float[]{0.5718280481994695f, 0.24070317428066512f, -0.3737913482606834f, -0.06726932177492717f, -0.6980531615588608f})))
                .filter("TEXT_MATCH(text, 'Turing London')")
                .annsField("embeddings")
                .topK(3)
                .outputFields(Collections.singletonList("text"))
                .build());

        System.out.println("Search results:");
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("ID: %d, Score: %f, %s\n", (long)result.getId(), result.getScore(), result.getEntity().toString());
            }
        }

        QueryResp queryResp = client.query(QueryReq.builder()
                .collectionName("keyword_search_collection")
                .filter("TEXT_MATCH(text, 'Turing') and TEXT_MATCH(text, 'England')")
                .outputFields(Arrays.asList("id", "text"))
                .build()
        );

        System.out.println("Query results:");
        List<QueryResp.QueryResult> results = queryResp.getQueryResults();
        for (QueryResp.QueryResult result : results) {
            System.out.println(result.getEntity());
        }

        client.close();
    }
}
