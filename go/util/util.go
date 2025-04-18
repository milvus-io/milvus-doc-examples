package util

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func GetClient(ctx context.Context) (*milvusclient.Client, error) {
	client, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: "localhost:19530",
		APIKey:  "root:Milvus",
	})
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	return client, nil
}

func CreateCollection(collectionName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("docId").WithDataType(entity.FieldTypeInt64))

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema))
	if err != nil {
		fmt.Println(err.Error())
	}

	colorColumn := column.NewColumnString("color", []string{
		"pink_8682", "red_7025", "orange_6781", "pink_9298", "red_4794", "yellow_4222", "red_9392", "grey_8510", "white_9381", "purple_4976",
	})
	likesColumn := column.NewColumnInt64("likes", []int64{
		10, 56, 32, 6, 654, 122, 42, 98, 56, 176,
	})

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).
		WithInt64Column("id", []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}).
		WithFloatVectorColumn("vector", 5, [][]float32{
			{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592},
			{0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104},
			{0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592},
			{0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995, 0.95791889146345},
			{0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184, 0.30337481143159106},
			{0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955},
			{0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192, -0.8984947637863987},
			{-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052},
			{0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872, -0.6140360785406336},
			{0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717, -0.6980531615588608},
		}).
		WithInt64Column("docId", []int64{1, 3, 2, 3, 3, 5, 4, 2, 1, 3}).
		WithColumns(colorColumn, likesColumn),
	)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	err = client.CreatePartition(ctx, milvusclient.NewCreatePartitionOption(collectionName, "partitionA"))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	colorColumn = column.NewColumnString("color", []string{
		"pink_8682", "red_7025", "orange_6781", "pink_9298", "red_4794", "yellow_4222", "red_9392", "grey_8510", "white_9381", "purple_4976",
	})

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).
		WithPartition("partitionA").
		WithInt64Column("id", []int64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}).
		WithFloatVectorColumn("vector", 5, [][]float32{
			{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592},
			{0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104},
			{0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592},
			{0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995, 0.95791889146345},
			{0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184, 0.30337481143159106},
			{0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955},
			{0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192, -0.8984947637863987},
			{-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052},
			{0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872, -0.6140360785406336},
			{0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717, -0.6980531615588608},
		}).
		WithInt64Column("docId", []int64{1, 3, 2, 3, 3, 5, 4, 2, 1, 3}).
		WithColumns(colorColumn),
	)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	indexTask, err := client.CreateIndex(ctx, milvusclient.NewCreateIndexOption(collectionName, "vector",
		index.NewAutoIndex(index.MetricType(entity.IP))))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	err = indexTask.Await(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handler err
	}

	FlushLoadCollection(client, collectionName)
}

func DropCollection(collectionName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
}

func FlushLoadCollection(client *milvusclient.Client, collectionName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	flushTask, err := client.Flush(ctx, milvusclient.NewFlushOption(collectionName))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	err = flushTask.Await(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	// sync wait collection to be loaded
	err = loadTask.Await(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
}
