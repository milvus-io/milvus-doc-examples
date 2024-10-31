package org.example.collection;

import io.milvus.v2.service.partition.request.ListPartitionsReq;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

import io.milvus.v2.service.partition.request.CreatePartitionReq;

import io.milvus.v2.service.partition.request.HasPartitionReq;

import io.milvus.v2.service.partition.request.LoadPartitionsReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;

import io.milvus.v2.service.partition.request.ReleasePartitionsReq;

import io.milvus.v2.service.partition.request.DropPartitionReq;

import java.util.*;


public class ManagePartition {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }

    private static void listPartitions() {
        ListPartitionsReq listPartitionsReq = ListPartitionsReq.builder()
                .collectionName("quick_setup")
                .build();

        List<String> partitionNames = client.listPartitions(listPartitionsReq);
        System.out.println(partitionNames);
    }

    private static void createPartition() {
        CreatePartitionReq createPartitionReq = CreatePartitionReq.builder()
                .collectionName("quick_setup")
                .partitionName("partitionA")
                .build();

        client.createPartition(createPartitionReq);

        ListPartitionsReq listPartitionsReq = ListPartitionsReq.builder()
                .collectionName("quick_setup")
                .build();

        List<String> partitionNames = client.listPartitions(listPartitionsReq);
        System.out.println(partitionNames);
    }

    private static void hasPartition() {
        HasPartitionReq hasPartitionReq = HasPartitionReq.builder()
                .collectionName("quick_setup")
                .partitionName("partitionA")
                .build();

        Boolean hasPartitionRes = client.hasPartition(hasPartitionReq);
        System.out.println(hasPartitionRes);
    }
    private static void loadPartition() {
        LoadPartitionsReq loadPartitionsReq = LoadPartitionsReq.builder()
                .collectionName("quick_setup")
                .partitionNames(Collections.singletonList("partitionA"))
                .build();

        client.loadPartitions(loadPartitionsReq);

        GetLoadStateReq getLoadStateReq = GetLoadStateReq.builder()
                .collectionName("quick_setup")
                .partitionName("partitionA")
                .build();

        Boolean getLoadStateRes = client.getLoadState(getLoadStateReq);
        System.out.println(getLoadStateRes);
    }

    private static void releasePartition() {
        ReleasePartitionsReq releasePartitionsReq = ReleasePartitionsReq.builder()
                .collectionName("quick_setup")
                .partitionNames(Collections.singletonList("partitionA"))
                .build();

        client.releasePartitions(releasePartitionsReq);

        GetLoadStateReq getLoadStateReq = GetLoadStateReq.builder()
                .collectionName("quick_setup")
                .partitionName("partitionA")
                .build();

        Boolean getLoadStateRes = client.getLoadState(getLoadStateReq);
        System.out.println(getLoadStateRes);
    }

    private static void dropPartition() {
        ReleasePartitionsReq releasePartitionsReq = ReleasePartitionsReq.builder()
                .collectionName("quick_setup")
                .partitionNames(Collections.singletonList("partitionA"))
                .build();

        client.releasePartitions(releasePartitionsReq);

        DropPartitionReq dropPartitionReq = DropPartitionReq.builder()
                .collectionName("quick_setup")
                .partitionName("partitionA")
                .build();

        client.dropPartition(dropPartitionReq);

        ListPartitionsReq listPartitionsReq = ListPartitionsReq.builder()
                .collectionName("quick_setup")
                .build();

        List<String> partitionNames = client.listPartitions(listPartitionsReq);
        System.out.println(partitionNames);
    }

    public static void main(String[] args) {
        listPartitions();
        createPartition();
        hasPartition();
        loadPartition();
        releasePartition();
//        dropPartition();
    }
}
