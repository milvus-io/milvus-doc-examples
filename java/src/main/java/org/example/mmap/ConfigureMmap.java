package org.example.mmap;

import io.milvus.param.Constant;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.*;
import io.milvus.v2.service.index.request.AlterIndexPropertiesReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfigureMmap {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }

    private static void configField() {
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

        Map<String, String> typeParams = new HashMap<String, String>() {{
            put(Constant.MMAP_ENABLED, "false");
        }};
        schema.addField(AddFieldReq.builder()
                .fieldName("doc_chunk")
                .dataType(DataType.VarChar)
                .maxLength(512)
                .typeParams(typeParams)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName("my_collection")
                .build());

        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("my_collection")
                .collectionSchema(schema)
                .build();
        client.createCollection(req);

        client.alterCollectionField(AlterCollectionFieldReq.builder()
                .collectionName("my_collection")
                .fieldName("doc_chunk")
                .property(Constant.MMAP_ENABLED, "true")
                .build());
    }

    private static void configIndex() {
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
                .fieldName("title")
                .dataType(DataType.VarChar)
                .maxLength(512)
                .build());

        List<IndexParam> indexParams = new ArrayList<>();
        indexParams.add(IndexParam.builder()
                .fieldName("vector")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.COSINE)
                .build());

        Map<String, Object> extraParams = new HashMap<String, Object>() {{
            put(Constant.MMAP_ENABLED, false);
        }};
        indexParams.add(IndexParam.builder()
                .fieldName("title")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .extraParams(extraParams)
                .build());

        client.dropCollection(DropCollectionReq.builder()
                .collectionName("my_collection")
                .build());

        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("my_collection")
                .collectionSchema(schema)
                .indexParams(indexParams)
                .build();
        client.createCollection(req);
        client.releaseCollection(ReleaseCollectionReq.builder()
                .collectionName("my_collection")
                .build());

        client.alterIndexProperties(AlterIndexPropertiesReq.builder()
                .collectionName("my_collection")
                .indexName("title")
                .property(Constant.MMAP_ENABLED, "true")
                .build());
    }

    private static void configCollection() {
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

        client.dropCollection(DropCollectionReq.builder()
                .collectionName("my_collection")
                .build());

        CreateCollectionReq req = CreateCollectionReq.builder()
                .collectionName("my_collection")
                .collectionSchema(schema)
                .property(Constant.MMAP_ENABLED, "false")
                .build();
        client.createCollection(req);

        client.alterCollectionProperties(AlterCollectionPropertiesReq.builder()
                .collectionName("my_collection")
                .property(Constant.MMAP_ENABLED, "true")
                .build());
    }

    public static void main(String[] args) {
        configField();
        configIndex();
        configCollection();
    }
}
