package org.example.schema;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.ConsistencyLevel;
import io.milvus.v2.common.DataType;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.AddFieldReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;

import java.util.ArrayList;
import java.util.List;

public class SchemaDesign {
    private static final MilvusClientV2 client;
    static {
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .build());
    }

    public static void main(String[] args) {
        // schema
        CreateCollectionReq.CollectionSchema schema = client.createSchema();

        schema.addField(AddFieldReq.builder()
                .fieldName("article_id")
                .dataType(DataType.Int64)
                .isPrimaryKey(true)
                .description("article id")
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("title")
                .dataType(DataType.VarChar)
                .maxLength(200)
                .description("article title")
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("author_info")
                .dataType(DataType.JSON)
                .description("author information")
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("publish_ts")
                .dataType(DataType.Int32)
                .description("publish timestamp")
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("image_url")
                .dataType(DataType.VarChar)
                .maxLength(500)
                .description("image URL")
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("image_vector")
                .dataType(DataType.FloatVector)
                .dimension(768)
                .description("image vector")
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("summary")
                .dataType(DataType.VarChar)
                .maxLength(1000)
                .description("article summary")
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("summary_dense_vector")
                .dataType(DataType.FloatVector)
                .dimension(768)
                .description("summary dense vector")
                .build());

        schema.addField(AddFieldReq.builder()
                .fieldName("summary_sparse_vector")
                .dataType(DataType.SparseFloatVector)
                .description("summary sparse vector")
                .build());

        // indexes
        List<IndexParam> indexes = new ArrayList<>();
        indexes.add(IndexParam.builder()
                .fieldName("image_vector")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.IP)
                .build());

        indexes.add(IndexParam.builder()
                .fieldName("summary_dense_vector")
                .indexType(IndexParam.IndexType.AUTOINDEX)
                .metricType(IndexParam.MetricType.IP)
                .build());

        indexes.add(IndexParam.builder()
                .fieldName("summary_sparse_vector")
                .indexType(IndexParam.IndexType.SPARSE_INVERTED_INDEX)
                .metricType(IndexParam.MetricType.IP)
                .build());

        indexes.add(IndexParam.builder()
                .fieldName("publish_ts")
                .indexType(IndexParam.IndexType.INVERTED)
                .build());

        String collectionName = "my_schema_design";
        CreateCollectionReq requestCreate = CreateCollectionReq.builder()
                .collectionName(collectionName)
                .collectionSchema(schema)
                .indexParams(indexes)
                .build();
        client.createCollection(requestCreate);

        DescribeCollectionResp descResp = client.describeCollection(DescribeCollectionReq.builder()
                .collectionName(collectionName)
                .build());
        System.out.println(descResp);
    }
}
