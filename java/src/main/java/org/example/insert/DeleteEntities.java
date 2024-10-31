package org.example.insert;

import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.DeleteReq;
import io.milvus.v2.service.vector.response.DeleteResp;

import java.util.Arrays;


public class DeleteEntities {
    private static void deleteByExpr() {
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());

        DeleteResp deleteResp = client.delete(DeleteReq.builder()
                .collectionName("quick_setup")
                .filter("color in ['red_3314', 'purple_7392']")
                .build());
        System.out.println(deleteResp);
    }
    private static void deleteByPk() {
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());

        DeleteResp deleteResp = client.delete(DeleteReq.builder()
                .collectionName("quick_setup")
                .ids(Arrays.asList(18, 19))
                .build());
        System.out.println(deleteResp);
    }

    private static void deleteInPartition() {
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());

        DeleteResp deleteResp = client.delete(DeleteReq.builder()
                .collectionName("quick_setup")
                .ids(Arrays.asList(18, 19))
                .partitionName("partitionA")
                .build());
        System.out.println(deleteResp);
    }


    public static void main(String[] args) {
        deleteByExpr();
        deleteByPk();
        deleteInPartition();
    }
}
