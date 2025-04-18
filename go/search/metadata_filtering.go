package search

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func MetadataFiltering() {
	createCollectionMetadata()
	defer util.DropCollection("my_collection")

	comparisonOp()
	termOp()
	matchOp()
	arithmeticOp()
	jsonOp()
	arrayOp()
	multiConditions()
}

func createCollectionMetadata() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true),
	).WithField(entity.NewField().
		WithName("vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(4),
	).WithField(entity.NewField().
		WithName("color").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(1000),
	).WithField(entity.NewField().
		WithName("price").
		WithDataType(entity.FieldTypeFloat),
	).WithField(entity.NewField().
		WithName("inventory").
		WithDataType(entity.FieldTypeJSON),
	).WithField(entity.NewField().
		WithName("sales_volume").
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeInt64).
		WithMaxCapacity(10),
	).WithField(entity.NewField().
		WithName("description").
		WithDataType(entity.FieldTypeVarChar).
		WithEnableMatch(true).
		WithEnableAnalyzer(true).
		WithMaxLength(1000),
	)

	vectorIndex := index.NewAutoIndex(entity.COSINE)
	indexOption := milvusclient.NewCreateIndexOption("my_collection", "vector", vectorIndex)

	err = client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("my_collection", schema).
		WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handler err
	}

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithInt64Column("id", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}).
		WithFloatVectorColumn("vector", 4, [][]float32{
			{0.1, 0.2, 0.3, 0.4},
			{0.2, 0.3, 0.4, 0.5},
			{0.3, 0.4, 0.5, 0.6},
			{0.4, 0.5, 0.6, 0.7},
			{0.5, 0.6, 0.7, 0.8},
			{0.6, 0.7, 0.8, 0.9},
			{0.7, 0.8, 0.9, 1.0},
			{0.8, 0.9, 1.0, 1.},
			{0.9, 1.0, 1.1, 1.2},
			{1.0, 1.1, 1.2, 1.3},
		}).
		WithVarcharColumn("color", []string{
			"pink_8682", "red_7025", "orange_6781", "pink_9298", "red_4794", "yellow_4222", "red_9392", "grey_8510", "white_9381", "purple_4976",
		}).
		WithVarcharColumn("description", []string{
			"Sony Xperia 1 VI is a flagship Android smartphone released in 2024 with a 6.5-inch LTPO OLED display",
			"Galaxy S24 Ultra, Samsung’s latest flagship smartphone.",
			"Galaxy Fold features the world’s first 7.3-inch Infinity Flex Display.",
			"Surface Duo 2, now with lightning-fast 5G(Footnote1) and dynamic triple lens camera.",
			"iPhone 15 Pro, A new chip designed for better gaming and other 'pro' features.",
			"The Microsoft Surface Duo seems at first like the perfect little device for this new work-from-home world.",
			"The iPhone 14 is a smartphone from Apple Inc. that comes in various colors and sizes.",
			"The Dell Inspiron 15 3000 laptop is equipped with a powerful Intel Core i5-1135G7 Quad-Core Processor, 12GB RAM and 256GB SSD storage.",
			"The iPhone 16 features a 6.1-inch OLED display, is powered by Apple's A18 processor, and has dual cameras at the back.",
			"The iPad is a brand of iOS- and iPadOS-based tablet computers that are developed and marketed by Apple.",
		}).
		WithColumns(
			column.NewColumnFloat("price", []float32{
				593, 196, 862, 991, 327, 996, 848, 241, 597, 450,
			}),
			column.NewColumnJSONBytes("inventory", [][]byte{
				[]byte(`{"brand": "Sony", "quantity": 310, "previous_sales": [451, 348, 224]}`),
				[]byte(`{"brand": "Samsung", "quantity": 274, "previous_sales": [315, 133, 109]}`),
				[]byte(`{"brand": "Samsung", "quantity": 103, "previous_sales": [232, 254, 275]}`),
				[]byte(`{"brand": "Microsoft", "quantity": 175, "previous_sales": [288, 169, 112]}`),
				[]byte(`{"brand": "Apple", "quantity": 193, "previous_sales": [225, 286, 202]}`),
				[]byte(`{"brand": "Microsoft", "quantity": 376, "previous_sales": [254, 275, 232]}`),
				[]byte(`{"brand": "Apple", "quantity": 61, "previous_sales": [312, 254, 367]}`),
				[]byte(`{"brand": "Dell", "quantity": 248, "previous_sales": [318, 238, 127]}`),
				[]byte(`{"brand": "Apple", "quantity": 351, "previous_sales": [482, 105, 130]}`),
				[]byte(`{"brand": "Apple", "quantity": 268, "previous_sales": [456, 271, 479]}`),
			}),
			column.NewColumnInt64Array("sales_volume", [][]int64{
				{161, 81, 51},
				{126, 126, 125, 96, 155},
				{124, 117, 90, 188},
				{133, 92, 181, 61, 193},
				{155, 161, 106, 86, 99},
				{173, 151, 148},
				{59, 156, 126, 60, 177},
				{105, 126, 114, 132},
				{150, 150, 73},
				{190, 149, 85, 79, 80},
			}),
		))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	flushTask, err := client.Flush(ctx, milvusclient.NewFlushOption("my_collection"))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}
	err = flushTask.Await(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("my_collection"))
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

func comparisonOp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("500 < price < 900").
		WithOutputFields("id", "color", "price"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("inventory[\"quantity\"] >= 250").
		WithOutputFields("id", "color", "price", "inventory"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("inventory: ", resultSet.GetColumn("inventory").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("sales_volume[0] >= 150").
		WithOutputFields("id", "color", "price", "sales_volume"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("sales_volume: ", resultSet.GetColumn("sales_volume").FieldData().GetScalars())
}

func termOp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("color not in [\"red_7025\",\"red_4794\",\"red_9392\"]").
		WithOutputFields("id", "color", "price"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("inventory[\"brand\"] in [\"Apple\"]").
		WithOutputFields("id", "color", "price", "inventory"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("inventory: ", resultSet.GetColumn("inventory").FieldData().GetScalars())
}

func matchOp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("color like \"red%\"").
		WithOutputFields("id", "color", "price"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("inventory[\"brand\"] like \"S%\"").
		WithOutputFields("id", "color", "price", "inventory"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("inventory: ", resultSet.GetColumn("inventory").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("TEXT_MATCH(description, \"Apple iPhone\")").
		WithOutputFields("id", "description"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("description: ", resultSet.GetColumn("description").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("TEXT_MATCH(description, \"chip\") and TEXT_MATCH(description, \"iPhone\")").
		WithOutputFields("id", "description"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("description: ", resultSet.GetColumn("description").FieldData().GetScalars())
}

func arithmeticOp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("200 <= price*0.5 and price*0.5 <= 300").
		WithOutputFields("id", "price"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("inventory[\"quantity\"] * 2 > 600").
		WithOutputFields("id", "color", "price", "inventory"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("inventory: ", resultSet.GetColumn("inventory").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("sales_volume[0]*2 > 300").
		WithOutputFields("id", "color", "price", "sales_volume"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("sales_volume: ", resultSet.GetColumn("sales_volume").FieldData().GetScalars())
}

func jsonOp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("JSON_CONTAINS(inventory[\"previous_sales\"], 232)").
		WithOutputFields("id", "color", "price", "inventory"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("inventory: ", resultSet.GetColumn("inventory").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("JSON_CONTAINS_ALL(inventory[\"previous_sales\"], [232, 254, 275])").
		WithOutputFields("id", "color", "price", "inventory"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("inventory: ", resultSet.GetColumn("inventory").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("JSON_CONTAINS_ANY(inventory[\"previous_sales\"], [232, 254, 275])").
		WithOutputFields("id", "color", "price", "inventory"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("inventory: ", resultSet.GetColumn("inventory").FieldData().GetScalars())
}

func arrayOp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("ARRAY_CONTAINS(sales_volume, 161)").
		WithOutputFields("id", "color", "price", "sales_volume"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("sales_volume: ", resultSet.GetColumn("sales_volume").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("ARRAY_CONTAINS_ALL(sales_volume, [150, 150])").
		WithOutputFields("id", "color", "price", "sales_volume"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("sales_volume: ", resultSet.GetColumn("sales_volume").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("ARRAY_CONTAINS_ANY(sales_volume, [150, 190, 90])").
		WithOutputFields("id", "color", "price", "sales_volume"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("sales_volume: ", resultSet.GetColumn("sales_volume").FieldData().GetScalars())

	resultSet, err = client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("ARRAY_LENGTH(sales_volume) == 3").
		WithOutputFields("id", "color", "price", "sales_volume"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("sales_volume: ", resultSet.GetColumn("sales_volume").FieldData().GetScalars())
}

func multiConditions() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	resultSet, err := client.Query(ctx, milvusclient.NewQueryOption("my_collection").
		WithFilter("color like \"red%\" and price < 500 and inventory[\"brand\"] in [\"Apple\"] and sales_volume[0] > 100").
		WithOutputFields("id", "color", "price", "inventory", "sales_volume"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	fmt.Println("id: ", resultSet.GetColumn("id").FieldData().GetScalars())
	fmt.Println("color: ", resultSet.GetColumn("color").FieldData().GetScalars())
	fmt.Println("price: ", resultSet.GetColumn("price").FieldData().GetScalars())
	fmt.Println("inventory: ", resultSet.GetColumn("inventory").FieldData().GetScalars())
	fmt.Println("sales_volume: ", resultSet.GetColumn("sales_volume").FieldData().GetScalars())
}
