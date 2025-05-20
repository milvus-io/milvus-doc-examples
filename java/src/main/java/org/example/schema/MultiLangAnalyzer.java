package org.example.schema;

import com.google.gson.JsonObject;
import io.milvus.common.clientenum.FunctionType;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;
import io.milvus.v2.service.utility.request.FlushReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.EmbeddedText;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class MultiLangAnalyzer {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static void createCollectionWithAnalyzer(Map<String, Object> analyzerParams) {
        CreateCollectionReq.CollectionSchema collectionSchema = CreateCollectionReq.CollectionSchema.builder()
                .build();
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(true)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("language")
                .dataType(DataType.VarChar)
                .maxLength(255)
                .build());

        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(8192)
                .enableAnalyzer(true)
                .multiAnalyzerParams(analyzerParams)
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("sparse")
                .dataType(DataType.SparseFloatVector)
                .build());

        CreateCollectionReq.Function function = CreateCollectionReq.Function.builder()
                .functionType(FunctionType.BM25)
                .name("text_to_vector")
                .inputFieldNames(Collections.singletonList("text"))
                .outputFieldNames(Collections.singletonList("sparse"))
                .build();
        collectionSchema.addFunction(function);

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName("sparse")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.BM25)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName("multilingual_documents")
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("multilingual_documents")
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .build();
        client.createCollection(requestCreate);

        System.out.println("Collection created");
    }

    public static void main(String[] args) {
        // english
        Map<String, Object> analyzerParams = new HashMap<>();
        analyzerParams.put("analyzers", new HashMap<String, Object>() {{
            put("english", new HashMap<String, Object>() {{
                put("type", "english");
            }});
            put("chinese", new HashMap<String, Object>() {{
                put("type", "chinese");
            }});
            put("default", new HashMap<String, Object>() {{
                put("tokenizer", "icu");
            }});
        }});
        analyzerParams.put("by_field", "language");
        analyzerParams.put("alias", new HashMap<String, Object>() {{
            put("cn", "chinese");
            put("en", "english");
        }});
        createCollectionWithAnalyzer(analyzerParams);

        List<String> texts = Arrays.asList(
                "Artificial intelligence is transforming technology",
                "Machine learning models require large datasets",
                "人工智能正在改变技术领域",
                "机器学习模型需要大型数据集"
        );
        List<String> languages = Arrays.asList(
                "english", "en", "chinese", "cn"
        );

        List<JsonObject> rows = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            JsonObject row = new JsonObject();
            row.addProperty("text", texts.get(i));
            row.addProperty("language", languages.get(i));
            rows.add(row);
        }
        client.insert(InsertReq.builder()
                .collectionName("multilingual_documents")
                .data(rows)
                .build());
        client.flush(FlushReq.builder().collectionNames(Collections.singletonList("multilingual_documents")).build());

        Map<String,Object> searchParams = new HashMap<>();
        searchParams.put("metric_type", "BM25");
        searchParams.put("analyzer_name", "english");
        searchParams.put("drop_ratio_search", 0);
        SearchResp searchResp = client.search(SearchReq.builder()
                .collectionName("multilingual_documents")
                .data(Collections.singletonList(new EmbeddedText("artificial intelligence")))
                .annsField("sparse")
                .topK(3)
                .searchParams(searchParams)
                .outputFields(Arrays.asList("text", "language"))
                .build());

        System.out.println("\n=== English Search Results ===");
        List<List<SearchResp.SearchResult>> searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("Score: %f, %s\n", result.getScore(), result.getEntity().toString());
            }
        }

        searchParams.put("analyzer_name", "cn");
        searchResp = client.search(SearchReq.builder()
                .collectionName("multilingual_documents")
                .data(Collections.singletonList(new EmbeddedText("人工智能")))
                .annsField("sparse")
                .topK(3)
                .searchParams(searchParams)
                .outputFields(Arrays.asList("text", "language"))
                .build());

        System.out.println("\n=== Chinese Search Results ===");
        searchResults = searchResp.getSearchResults();
        for (List<SearchResp.SearchResult> results : searchResults) {
            for (SearchResp.SearchResult result : results) {
                System.out.printf("Score: %f, %s\n", result.getScore(), result.getEntity().toString());
            }
        }

        client.close();
    }
}
