package org.example.schema;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.BinaryVec;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.SearchResp;

import java.util.*;

public class BinaryVector {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    private static void createCollection() {
        CreateCollectionReq.CollectionSchema schema = client.createSchema();
        schema.setEnableDynamicField(true);
        schema.addField(AddFieldReq.builder()
                .fieldName("pk")
                .dataType(DataType.VarChar)
                .isPrimaryKey(true)
                .autoID(true)
                .maxLength(100)
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("binary_vector")
                .dataType(DataType.BinaryVector)
                .dimension(16)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        Map<String,Object> extraParams = new HashMap<>();
        extraParams.put("nlist",128);
        indexParams.add(IndexParam.builder()
                .fieldName("binary_vector")
                .indexName("binary_vector_index")
                .indexType(IndexParam.IndexType.BIN_IVF_FLAT)
                .metricType(IndexParam.MetricType.HAMMING)
                .extraParams(extraParams)
                .build());

        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName("my_binary_collection")
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build();
        client.createCollection(requestCreate);
    }

    private static byte[] convertBoolArrayToBytes(boolean[] booleanArray) {
        byte[] byteArray = new byte[booleanArray.length / Byte.SIZE];
        for (int i = 0; i < booleanArray.length; i++) {
            if (booleanArray[i]) {
                int index = i / Byte.SIZE;
                int shift = i % Byte.SIZE;
                byteArray[index] |= (byte) (1 << shift);
            }
        }

        return byteArray;
    }

    private static void insert() {
        List<JsonObject> rows = new ArrayList<>();
        Gson gson = new Gson();
        {
            boolean[] boolArray = {true, false, false, true, true, false, true, true, false, true, false, false, true, true, false, true};
            JsonObject row = new JsonObject();
            row.add("binary_vector", gson.toJsonTree(convertBoolArrayToBytes(boolArray)));
            rows.add(row);
        }
        {
            boolean[] boolArray = {false, true, false, true, false, true, false, false, true, true, false, false, true, true, false, true};
            JsonObject row = new JsonObject();
            row.add("binary_vector", gson.toJsonTree(convertBoolArrayToBytes(boolArray)));
            rows.add(row);
        }

        InsertResp insertR = client.insert(InsertReq.builder()
                .collectionName("my_binary_collection")
                .data(rows)
                .build());
    }

    private static void search() {
        Map<String,Object> searchParams = new HashMap<>();
        searchParams.put("nprobe",10);

        boolean[] boolArray = {true, false, false, true, true, false, true, true, false, true, false, false, true, true, false, true};
        BinaryVec queryVector = new BinaryVec(convertBoolArrayToBytes(boolArray));

        SearchResp searchR = client.search(SearchReq.builder()
                .collectionName("my_binary_collection")
                .data(Collections.singletonList(queryVector))
                .annsField("binary_vector")
                .searchParams(searchParams)
                .topK(5)
                .outputFields(Collections.singletonList("pk"))
                .build());

        System.out.println(searchR.getSearchResults());
    }


    public static void main(String[] args) {
        createCollection();
        insert();
        search();
    }
}
