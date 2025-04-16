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

func HybridSearch() {
	createCollectionHybrid()
	defer dropCollectionHybrid()

	standardHybridSearch()
}

func createCollectionHybrid() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	schema := entity.NewSchema().WithDynamicFieldEnabled(true)
	schema.WithField(entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true),
	).WithField(entity.NewField().
		WithName("text").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(1000),
	).WithField(entity.NewField().
		WithName("sparse").
		WithDataType(entity.FieldTypeSparseVector),
	).WithField(entity.NewField().
		WithName("dense").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(5),
	)

	indexOption1 := milvusclient.NewCreateIndexOption("hybrid_search_collection", "sparse",
		index.NewSparseInvertedIndex(entity.IP, 0.2))
	indexOption2 := milvusclient.NewCreateIndexOption("hybrid_search_collection", "dense",
		index.NewAutoIndex(index.MetricType(entity.IP)))

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("hybrid_search_collection", schema).
			WithIndexOptions(indexOption1, indexOption2))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	v := make([]entity.SparseEmbedding, 0, 3)
	sparseVector1, _ := entity.NewSliceSparseEmbedding([]uint32{9637, 4399, 3573}, []float32{0.30856525997853057, 0.19771651149001523, 0.1576378135})
	v = append(v, sparseVector1)
	sparseVector2, _ := entity.NewSliceSparseEmbedding([]uint32{6959, 1729, 5263}, []float32{0.31025067641541815, 0.8265339135915016, 0.68647322132})
	v = append(v, sparseVector2)
	sparseVector3, _ := entity.NewSliceSparseEmbedding([]uint32{1220, 7335}, []float32{0.15303302147479103, 0.9436728846033107})
	v = append(v, sparseVector3)
	sparseColumn := column.NewColumnSparseVectors("sparse", v)

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("hybrid_search_collection").
		WithInt64Column("id", []int64{0, 1, 2}).
		WithVarcharColumn("text", []string{
			"Artificial intelligence was founded as an academic discipline in 1956.",
			"Alan Turing was the first person to conduct substantial research in AI.",
			"Born in Maida Vale, London, Turing was raised in southern England.",
		}).
		WithFloatVectorColumn("dense", 5, [][]float32{
			{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592},
			{0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104},
			{0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592},
		}).
		WithColumns(sparseColumn))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	_, err = client.Flush(ctx, milvusclient.NewFlushOption("hybrid_search_collection"))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("hybrid_search_collection"))
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

func dropCollectionHybrid() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("hybrid_search_collection"))
}

func standardHybridSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}
	sparseVector, _ := entity.NewSliceSparseEmbedding([]uint32{3573, 5263}, []float32{0.34701499, 0.263937551})

	request1 := milvusclient.NewAnnRequest("dense", 2, entity.FloatVector(queryVector)).
		WithAnnParam(index.NewIvfAnnParam(10)).
		WithSearchParam(index.MetricTypeKey, "IP")
	annParam := index.NewSparseAnnParam()
	annParam.WithDropRatio(0.2)
	request2 := milvusclient.NewAnnRequest("sparse", 2, sparseVector).
		WithAnnParam(annParam).
		WithSearchParam(index.MetricTypeKey, "IP")

	reranker := milvusclient.NewWeightedReranker([]float64{0.8, 0.3})
	// reranker := milvusclient.NewRRFReranker()

	resultSets, err := client.HybridSearch(ctx, milvusclient.NewHybridSearchOption(
		"hybrid_search_collection",
		2,
		request1,
		request2,
	).WithReranker(reranker))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
	}
}
