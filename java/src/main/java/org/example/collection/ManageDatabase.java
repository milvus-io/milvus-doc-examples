package org.example.collection;

import io.milvus.v2.service.database.response.ListDatabasesResp;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;

import io.milvus.v2.service.database.request.CreateDatabaseReq;

import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;

import io.milvus.v2.service.database.request.DropDatabaseReq;


public class ManageDatabase {
    private static final MilvusClientV2 client;
    static {
        String CLUSTER_ENDPOINT = "http://localhost:19530";
        String TOKEN = "root:Milvus";
        client = new MilvusClientV2(ConnectConfig.builder()
                .uri(CLUSTER_ENDPOINT)
                .token(TOKEN)
                .build());
    }

    private static void createDatabase() {
        ListDatabasesResp listDatabasesResp = client.listDatabases();

        System.out.println(listDatabasesResp.getDatabaseNames());

        // 2. Create database
        CreateDatabaseReq createDatabaseReq = CreateDatabaseReq.builder()
                .databaseName("my_database")
                .build();

        client.createDatabase(createDatabaseReq);

        // 3. Describe database
        DescribeDatabaseReq describeDatabaseReq = DescribeDatabaseReq.builder()
                .databaseName("my_database")
                .build();

        DescribeDatabaseResp describeDatabaseResp = client.describeDatabase(describeDatabaseReq);
        System.out.println(describeDatabaseResp);
    }
    private static void useDatabase() throws InterruptedException {
        client.useDatabase("my_database");
    }

    private static void dropDatabase() {
        DropDatabaseReq dropDatabaseReq = DropDatabaseReq.builder()
                .databaseName("my_database")
                .build();

        client.dropDatabase(dropDatabaseReq);
    }

    public static void main(String[] args) throws InterruptedException {
        createDatabase();
        useDatabase();
        dropDatabase();
    }
}
