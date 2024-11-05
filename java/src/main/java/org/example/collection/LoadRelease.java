package org.example.collection;

import io.milvus.v2.service.collection.request.LoadCollectionReq;
import io.milvus.v2.service.collection.request.GetLoadStateReq;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.collection.request.ReleaseCollectionReq;

import java.util.Arrays;

public class LoadRelease {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }
    private static void loadCollection() {
        // 6. Load the collection
        LoadCollectionReq loadCollectionReq = LoadCollectionReq.builder()
                .collectionName("customized_setup_1")
                .build();

        client.loadCollection(loadCollectionReq);

        // 7. Get load state of the collection
        GetLoadStateReq loadStateReq = GetLoadStateReq.builder()
                .collectionName("customized_setup_1")
                .build();

        Boolean res = client.getLoadState(loadStateReq);
        System.out.println(res);
    }

    private static void loadField() {
        // 6. Load the collection
        LoadCollectionReq loadCollectionReq = LoadCollectionReq.builder()
                .collectionName("customized_setup_1")
                .loadFields(Arrays.asList("my_id", "my_vector"))
                .build();

        client.loadCollection(loadCollectionReq);

        // 7. Get load state of the collection
        GetLoadStateReq loadStateReq = GetLoadStateReq.builder()
                .collectionName("customized_setup_1")
                .build();

        Boolean res = client.getLoadState(loadStateReq);
        System.out.println(res);
    }

    private static void releaseCollection() {
        // 8. Release the collection
        ReleaseCollectionReq releaseCollectionReq = ReleaseCollectionReq.builder()
                .collectionName("customized_setup_1")
                .build();

        client.releaseCollection(releaseCollectionReq);

        GetLoadStateReq loadStateReq = GetLoadStateReq.builder()
                .collectionName("customized_setup_1")
                .build();
        Boolean res = client.getLoadState(loadStateReq);
        System.out.println(res);
    }

    public static void main(String[] args) {
        releaseCollection();
        loadCollection();
        releaseCollection();
        loadField();
    }
}
