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
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.data.BaseVector;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.SparseFloatVec;
import io.milvus.v2.service.vector.request.ranker.BaseRanker;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
import io.milvus.v2.service.vector.request.ranker.WeightedRanker;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class HybridSearch {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());
    }

    static private CreateCollectionReq.CollectionSchema createSchema() {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();
        schema.addField(AddFieldReq.builder()
                .fieldName("id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .autoID(false)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("text")
                .dataType(DataType.VarChar)
                .maxLength(1000)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("dense")
                .dataType(DataType.FloatVector)
                .dimension(768)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("sparse")
                .dataType(DataType.SparseFloatVector)
                .build());

        return schema;
    }

    static private List<IndexParam> createIndex() {
        Map<String, Object> denseParams = new HashMap<>();
        denseParams.put("nlist", 128);
        IndexParam indexParamForDenseField = IndexParam.builder()
                .fieldName("dense")
                .indexName("dense_index")
                .indexType(IndexParam.IndexType.IVF_FLAT)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(denseParams)
                .build();

        Map<String, Object> sparseParams = new HashMap<>();
        sparseParams.put("drop_ratio_build", 0.2);
        IndexParam indexParamForSparseField = IndexParam.builder()
                .fieldName("sparse")
                .indexName("sparse_index")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.IP)
                .extraParams(sparseParams)
                .build();

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(indexParamForDenseField);
        indexParams.add(indexParamForSparseField);

        return indexParams;
    }

    static private void createCollection() {
        client.dropCollection(DropCollectionReq.builder()
                .collectionName("hybrid_search_collection")
                .build());

        CreateCollectionReq.CollectionSchema schema = createSchema();
        List<IndexParam> indexParams = createIndex();

        CreateCollectionReq createCollectionReq = CreateCollectionReq.builder()
                .collectionName("hybrid_search_collection")
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build();
        client.createCollection(createCollectionReq);
    }

    static private void insert() {
        List<Float> dense1 = new ArrayList<>();
        List<Float> dense2 = new ArrayList<>();
        List<Float> dense3 = new ArrayList<>();
        Random rnd = new Random();
        for (int i = 0; i < 768; i++) {
            dense1.add(rnd.nextFloat());
            dense2.add(rnd.nextFloat());
            dense3.add(rnd.nextFloat());
        }
        SortedMap<Long, Float> sparse1 = new TreeMap<>();
        SortedMap<Long, Float> sparse2 = new TreeMap<>();
        SortedMap<Long, Float> sparse3 = new TreeMap<>();
        for (int i = 0; i < rnd.nextInt(10) + 10; ++i) {
            sparse1.put((long)rnd.nextInt(10000), rnd.nextFloat());
            sparse2.put((long)rnd.nextInt(10000), rnd.nextFloat());
            sparse3.put((long)rnd.nextInt(10000), rnd.nextFloat());
        }

        Gson gson = new Gson();
        JsonObject row1 = new JsonObject();
        row1.addProperty("id", 1);
        row1.addProperty("text", "Artificial intelligence was founded as an academic discipline in 1956.");
        row1.add("dense", gson.toJsonTree(dense1));
        row1.add("sparse", gson.toJsonTree(sparse1));

        JsonObject row2 = new JsonObject();
        row2.addProperty("id", 2);
        row2.addProperty("text", "Alan Turing was the first person to conduct substantial research in AI.");
        row2.add("dense", gson.toJsonTree(dense2));
        row2.add("sparse", gson.toJsonTree(sparse2));

        JsonObject row3 = new JsonObject();
        row3.addProperty("id", 3);
        row3.addProperty("text", "Born in Maida Vale, London, Turing was raised in southern England.");
        row3.add("dense", gson.toJsonTree(dense3));
        row3.add("sparse", gson.toJsonTree(sparse3));

        List<JsonObject> data = Arrays.asList(row1, row2, row3);
        InsertReq insertReq = InsertReq.builder()
                .collectionName("hybrid_search_collection")
                .data(data)
                .build();

        InsertResp insertResp = client.insert(insertReq);
    }

    private static void hybridSearch() {
        List<Float> dense = new ArrayList<>();
        Random rnd = new Random();
        for (int i = 0; i < 768; i++) {
            dense.add(rnd.nextFloat());
        }
        SortedMap<Long, Float> sparse = new TreeMap<>();
        for (int i = 0; i < rnd.nextInt(10) + 10; ++i) {
            sparse.put((long)rnd.nextInt(10000), rnd.nextFloat());
        }

//        float[] dense = new float[]{-0.0475336798f,  0.0521207601f,  0.0904406682f, ...};
//        SortedMap<Long, Float> sparse = new TreeMap<Long, Float>() {{
//            put(3573L, 0.34701499f);
//            put(5263L, 0.263937551f);
//            ...
//        }};


        List<BaseVector> queryDenseVectors = Collections.singletonList(new FloatVec(dense));
        List<BaseVector> querySparseVectors = Collections.singletonList(new SparseFloatVec(sparse));

        List<AnnSearchReq> searchRequests = new ArrayList<>();
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName("dense")
                .vectors(queryDenseVectors)
                .metricType(IndexParam.MetricType.IP)
                .params("{\"nprobe\": 10}")
                .topK(2)
                .build());
        searchRequests.add(AnnSearchReq.builder()
                .vectorFieldName("sparse")
                .vectors(querySparseVectors)
                .metricType(IndexParam.MetricType.IP)
                .params("{\"drop_ratio_build\": 0.2}")
                .topK(2)
                .build());

//        BaseRanker reranker = new WeightedRanker(Arrays.asList(0.8f, 0.3f));
        BaseRanker reranker = new RRFRanker(100);

        HybridSearchReq hybridSearchReq = HybridSearchReq.builder()
        .collectionName("hybrid_search_collection")
        .searchRequests(searchRequests)
        .ranker(reranker)
        .topK(2)
        .consistencyLevel(ConsistencyLevel.BOUNDED)
        .build();

        SearchResp searchResp = client.hybridSearch(hybridSearchReq);
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
        insert();
        hybridSearch();
    }
}
