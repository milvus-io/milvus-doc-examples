package org.example.collection;

import io.milvus.v2.common.DataType;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.service.collection.request.CreateCollectionReq;


public class QuickCollection {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }

    private static void simpleCreate() {
        // 2. Create a collection in quick setup mode
        CreateCollectionReq quickSetupReq = CreateCollectionReq.builder()
                .collectionName("quick_setup")
                .dimension(5)
                .build();

        client.createCollection(quickSetupReq);

        GetLoadStateReq quickSetupLoadStateReq = GetLoadStateReq.builder()
                .collectionName("quick_setup")
                .build();

        Boolean res = client.getLoadState(quickSetupLoadStateReq);
        System.out.println(res);
    }

    private static void quickCreate() {
        // 2. Create a collection in quick setup mode
        CreateCollectionReq customQuickSetupReq = CreateCollectionReq.builder()
                .collectionName("custom_quick_setup")
                .dimension(5)
                .primaryFieldName("my_id")
                .idType(DataType.VarChar)
                .maxLength(512)
                .vectorFieldName("my_vector")
                .metricType("L2")
                .autoID(true)
                .build();

        client.createCollection(customQuickSetupReq);

        GetLoadStateReq customQuickSetupLoadStateReq = GetLoadStateReq.builder()
                .collectionName("custom_quick_setup")
                .build();

        Boolean res = client.getLoadState(customQuickSetupLoadStateReq);
        System.out.println(res);
    }

    public static void main(String[] args) {
        simpleCreate();
        quickCreate();
    }
}
