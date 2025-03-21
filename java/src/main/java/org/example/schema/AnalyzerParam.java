package org.example.schema;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DropCollectionReq;

import java.util.*;

public class AnalyzerParam {
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
                .build());
        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("vector")
                .dataType(DataType.FloatVector)
                .dimension(128)
                .build());

        collectionSchema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(1000)
                .enableAnalyzer(true)
                .analyzerParams(analyzerParams)
                .enableMatch(true)
                .build());

        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.FLAT)
                .metricType(IndexParam.MetricType.L2)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName("my_analyzer_collection")
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("my_analyzer_collection")
                .collectionSchema(collectionSchema)
                .indexParams(indexes)
                .build();
        client.createCollection(requestCreate);
        System.out.println("Collection created");
    }

    public static void main(String[] args) {
        {
            // standard analyzer
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("type", "standard");
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // standard english
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("type", "english");
            analyzerParams.put("filter", Collections.singletonList("lowercase"));
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // english
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter",
                    Arrays.asList("lowercase",
                            new HashMap<String, Object>() {{
                                put("type", "stemmer");
                                put("language", "english");
                            }},
                            new HashMap<String, Object>() {{
                                put("type", "stop");
                                put("stop_words", Collections.singletonList("_english_"));
                            }}
                    )
            );
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // chinese
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "jieba");
            analyzerParams.put("filter", Collections.singletonList("cnalphanumonly"));
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // standard chinese
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("type", "chinese");
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // standard tokenizer
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // tokenizer
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter", Collections.singletonList("lowercase"));
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // tokenizer whitespace
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "whitespace");
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // tokenizer whitespace
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "whitespace");
            analyzerParams.put("filter", Collections.singletonList("lowercase"));
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // jieba
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "jieba");
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // ascii folding
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter", Collections.singletonList("asciifolding"));
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // alphanumonly
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter", Collections.singletonList("alphanumonly"));
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // cnalphanumonly
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter", Collections.singletonList("cnalphanumonly"));
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // cncharonly
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter", Collections.singletonList("cncharonly"));
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // length
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter",
                    Collections.singletonList(new HashMap<String, Object>() {{
                        put("type", "length");
                        put("max", 10);
                    }}));
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // stop
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter",
                    Collections.singletonList(
                            new HashMap<String, Object>() {{
                                put("type", "stop");
                                put("stop_words", Arrays.asList("of", "to", "_english_"));
                            }}
                    )
            );
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // decompounder
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter",
                    Collections.singletonList(
                            new HashMap<String, Object>() {{
                                put("type", "decompounder");
                                put("word_list", Arrays.asList("dampf", "schiff", "fahrt", "brot", "backen", "automat"));
                            }}
                    )
            );
            createCollectionWithAnalyzer(analyzerParams);
        }
        {
            // stemmer
            Map<String, Object> analyzerParams = new HashMap<>();
            analyzerParams.put("tokenizer", "standard");
            analyzerParams.put("filter",
                    Collections.singletonList(
                            new HashMap<String, Object>() {{
                                put("type", "stemmer");
                                put("language", "english");
                            }}
                    )
            );
            createCollectionWithAnalyzer(analyzerParams);
        }
    }
}
