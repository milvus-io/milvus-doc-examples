package org.example.search;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.common.clientenum.FunctionType;
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
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class FullTextSearch {
    public static void main(String[] args) {
        ConnectConfig config = ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build();
        MilvusClientV2 client = new MilvusClientV2(config);

        // Drop collection if exists
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("full_text_search_collection")
                .build());

        // Create collection
        CreateCollectionReq.CollectionSchema schema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(65535)
                .enableAnalyzer(true)
                .build());
        schema.addField(AddFieldReq.builder()
                .fieldName("sparse")
                .dataType(DataType.SparseFloatVector)
                .build());

        schema.addFunction(CreateCollectionReq.Function.builder()
                .functionType(FunctionType.BM25)
                .name("text_bm25_emb")
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList("sparse"))
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("drop_ratio_build", 0.2);
        indexes.add(IndexParam.builder()
                .fieldName("sparse")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.BM25) // to use full text search, metric type must be "BM25"
                .extraParams(extraParams)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("full_text_search_collection")
                .collectionSchema(schema)
                .indexParams(indexes)
                .consistencyLevel(ConsistencyLevel.BOUNDED)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");

        // Insert rows
        Gson gson = new Gson();
        List<JsonObject> rows = Arrays.asList(
                gson.fromJson("{\"text\": \"Artificial intelligence was founded as an academic discipline in 1956.\"}", JsonObject.class),
                gson.fromJson("{\"text\": \"Alan Turing was the first person to conduct substantial research in AI.\"}", JsonObject.class),
                gson.fromJson("{\"text\": \"Born in Maida Vale, London, Turing was raised in southern England.\"}", JsonObject.class)
        );

        client.insert(InsertReq.builder()
                .collectionName("full_text_search_collection")
                .data(rows)
                .build());

        Map<String,Object> searchParams = new HashMap<>();
        searchParams.put("drop_ratio_search", 0.2);
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName("full_text_search_collection")
                .data(Collections.singletonList(new EmbeddedText("Who started AI research?")))
                .annsField("sparse")
                .topK(3)
                .searchParams(searchParams)
                .outputFields(Collections.singletonList("text"))
                .build());

        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("ID: %d, Score: %f, %s\n", (long)result.getId(), result.getScore(), result.getEntity().toString());
            }
        }

        client.close();
    }
}
