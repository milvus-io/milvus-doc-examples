package org.example.insert;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.UpsertResp;

import java.util.*;

public class UpsertEntities {
    private static void upsertToCollection() {
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());

        Gson gson = new Gson();
        List<JsonObject> data = Arrays.asList(
                gson.fromJson("{\"id\": 0, \"vector\": [-0.619954382375778, 0.4479436794798608, -0.17493894838751745, -0.4248030059917294, -0.8648452746018911], \"color\": \"black_9898\"}", JsonObject.class),
                gson.fromJson("{\"id\": 1, \"vector\": [0.4762662251462588, -0.6942502138717026, -0.4490002642657902, -0.628696575798281, 0.9660395877041965], \"color\": \"red_7319\"}", JsonObject.class),
                gson.fromJson("{\"id\": 2, \"vector\": [-0.8864122635045097, 0.9260170474445351, 0.801326976181461, 0.6383943392381306, 0.7563037341572827], \"color\": \"white_6465\"}", JsonObject.class),
                gson.fromJson("{\"id\": 3, \"vector\": [0.14594326235891586, -0.3775407299900644, -0.3765479013078812, 0.20612075380355122, 0.4902678929632145], \"color\": \"orange_7580\"}", JsonObject.class),
                gson.fromJson("{\"id\": 4, \"vector\": [0.4548498669607359, -0.887610217681605, 0.5655081329910452, 0.19220509387904117, 0.016513983433433577], \"color\": \"red_3314\"}", JsonObject.class),
                gson.fromJson("{\"id\": 5, \"vector\": [0.11755001847051827, -0.7295149788999611, 0.2608115847524266, -0.1719167007897875, 0.7417611743754855], \"color\": \"black_9955\"}", JsonObject.class),
                gson.fromJson("{\"id\": 6, \"vector\": [0.9363032158314308, 0.030699901477745373, 0.8365910312319647, 0.7823840208444011, 0.2625222076909237], \"color\": \"yellow_2461\"}", JsonObject.class),
                gson.fromJson("{\"id\": 7, \"vector\": [0.0754823906014721, -0.6390658668265143, 0.5610517334334937, -0.8986261118798251, 0.9372056764266794], \"color\": \"white_5015\"}", JsonObject.class),
                gson.fromJson("{\"id\": 8, \"vector\": [-0.3038434006935904, 0.1279149203380523, 0.503958664270957, -0.2622661156746988, 0.7407627307791929], \"color\": \"purple_6414\"}", JsonObject.class),
                gson.fromJson("{\"id\": 9, \"vector\": [-0.7125086947677588, -0.8050968321012257, -0.32608864121785786, 0.3255654958645424, 0.26227968923834233], \"color\": \"brown_7231\"}", JsonObject.class)
        );

        UpsertReq upsertReq = UpsertReq.builder()
                .collectionName("quick_setup")
                .data(data)
                .build();

        UpsertResp upsertResp = client.upsert(upsertReq);
        System.out.println(upsertResp);
    }

    private static void upsertToPartition() {
        MilvusClientV2 client = new MilvusClientV2(ConnectConfig.builder()
                .uri("http://localhost:19530")
                .token("root:Milvus")
                .build());

        Gson gson = new Gson();
        List<JsonObject> data = Arrays.asList(
                gson.fromJson("{\"id\": 10, \"vector\": [0.06998888224297328, 0.8582816610326578, -0.9657938677934292, 0.6527905683627726, -0.8668460657158576], \"color\": \"black_3651\"}", JsonObject.class),
                gson.fromJson("{\"id\": 11, \"vector\": [0.6060703043917468, -0.3765080534566074, -0.7710758854987239, 0.36993888322346136, 0.5507513364206531], \"color\": \"grey_2049\"}", JsonObject.class),
                gson.fromJson("{\"id\": 12, \"vector\": [-0.9041813104515337, -0.9610546012461163, 0.20033003106083358, 0.11842506351635174, 0.8327356724591011], \"color\": \"blue_6168\"}", JsonObject.class),
                gson.fromJson("{\"id\": 13, \"vector\": [0.3202914977909075, -0.7279137773695252, -0.04747830871620273, 0.8266053056909548, 0.8277957187455489], \"color\": \"blue_1672\"}", JsonObject.class),
                gson.fromJson("{\"id\": 14, \"vector\": [0.2975811497890859, 0.2946936202691086, 0.5399463833894609, 0.8385334966677529, -0.4450543984655133], \"color\": \"pink_1601\"}", JsonObject.class),
                gson.fromJson("{\"id\": 15, \"vector\": [-0.04697464305600074, -0.08509022265734134, 0.9067184632552001, -0.2281912685064822, -0.9747503428652762], \"color\": \"yellow_9925\"}", JsonObject.class),
                gson.fromJson("{\"id\": 16, \"vector\": [-0.9363075919673911, -0.8153981031085669, 0.7943039120490902, -0.2093886809842529, 0.0771191335807897], \"color\": \"orange_9872\"}", JsonObject.class),
                gson.fromJson("{\"id\": 17, \"vector\": [-0.050451522820639916, 0.18931572752321935, 0.7522886192190488, -0.9071793089474034, 0.6032647330692296], \"color\": \"red_6450\"}", JsonObject.class),
                gson.fromJson("{\"id\": 18, \"vector\": [-0.9181544231141592, 0.6700755998126806, -0.014174674636136642, 0.6325780463623432, -0.49662222164032976], \"color\": \"purple_7392\"}", JsonObject.class),
                gson.fromJson("{\"id\": 19, \"vector\": [0.11426945899602536, 0.6089190684002581, -0.5842735738352236, 0.057050610092692855, -0.035163433018196244], \"color\": \"pink_4996\"}", JsonObject.class)
        );

        UpsertReq upsertReq = UpsertReq.builder()
                .collectionName("quick_setup")
                .partitionName("partitionA")
                .data(data)
                .build();

        UpsertResp upsertResp = client.upsert(upsertReq);
        System.out.println(upsertResp);
    }

    public static void main(String[] args) {
        upsertToCollection();
        upsertToPartition();
    }
}
