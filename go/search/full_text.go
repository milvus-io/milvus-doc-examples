package search

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func FullTextSearch() {
	createCollectionFull()
	defer dropCollectionFull()

	fullTextSearch()
}

func createCollectionFull() {
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
		WithIsPrimaryKey(true).
		WithIsAutoID(true),
	).WithField(entity.NewField().
		WithName("text").
		WithDataType(entity.FieldTypeVarChar).
		WithEnableAnalyzer(true).
		WithMaxLength(1000),
	).WithField(entity.NewField().
		WithName("sparse").
		WithDataType(entity.FieldTypeSparseVector),
	)

	function := entity.NewFunction().
		WithName("text_bm25_emb").
		WithInputFields("text").
		WithOutputFields("sparse").
		WithType(entity.FunctionTypeBM25)
	schema.WithFunction(function)

	indexOption := milvusclient.NewCreateIndexOption("my_collection", "sparse",
		index.NewAutoIndex(entity.MetricType(entity.BM25)))

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("my_collection", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("my_collection").
		WithVarcharColumn("text", []string{
			"information retrieval is a field of study.",
			"information retrieval focuses on finding relevant information in large datasets.",
			"data mining and information retrieval overlap in research.",
		}))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	_, err = client.Flush(ctx, milvusclient.NewFlushOption("my_collection"))
	if err != nil {
		fmt.Println(err.Error())
		// handle err
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

func dropCollectionFull() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	defer client.Close(ctx)

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("my_collection"))
}

func fullTextSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	annSearchParams := index.NewCustomAnnParam()
	annSearchParams.WithExtraParam("drop_ratio_search", 0.2)
	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"my_collection", // collectionName
		3,               // limit
		[]entity.Vector{entity.Text("whats the focus of information retrieval?")},
	).WithConsistencyLevel(entity.ClStrong).
		WithANNSField("sparse").
		WithAnnParam(annSearchParams).
		WithOutputFields("text"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		fmt.Println("IDs: ", resultSet.IDs.FieldData().GetScalars())
		fmt.Println("Scores: ", resultSet.Scores)
		fmt.Println("text: ", resultSet.GetColumn("text").FieldData().GetScalars())
	}
}
