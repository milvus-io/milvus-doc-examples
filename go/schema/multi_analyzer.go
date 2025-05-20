package schema

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func MultiAnalyzer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	multiAnalyzerParams := map[string]any{
		"analyzers": map[string]any{
			"english": map[string]string{"type": "english"},
			"chinese": map[string]string{"type": "chinese"},
			"default": map[string]string{"tokenizer": "icu"},
		},
		"by_field": "language",
		"alias": map[string]string{
			"cn": "chinese",
			"en": "english",
		},
	}
	fmt.Println(multiAnalyzerParams)

	schema := entity.NewSchema()

	schema.WithField(entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true).
		WithIsAutoID(true),
	).WithField(entity.NewField().
		WithName("language").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(255),
	).WithField(entity.NewField().
		WithName("text").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(8192).
		WithEnableAnalyzer(true).
		WithMultiAnalyzerParams(multiAnalyzerParams),
	).WithField(entity.NewField().
		WithName("sparse").
		WithDataType(entity.FieldTypeSparseVector),
	)

	function := entity.NewFunction()
	schema.WithFunction(function.WithName("text_to_vector").
		WithType(entity.FunctionTypeBM25).
		WithInputFields("text").
		WithOutputFields("sparse"))

	idx := index.NewAutoIndex(index.MetricType(entity.BM25))
	indexOption := milvusclient.NewCreateIndexOption("multilingual_documents", "sparse", idx)

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("multilingual_documents", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	column1 := column.NewColumnVarChar("text",
		[]string{
			"Artificial intelligence is transforming technology",
			"Machine learning models require large datasets",
			"人工智能正在改变技术领域",
			"机器学习模型需要大型数据集",
		})
	column2 := column.NewColumnVarChar("language",
		[]string{"english", "en", "chinese", "cn"})

	_, err = client.Insert(ctx, milvusclient.NewColumnBasedInsertOption("multilingual_documents").
		WithColumns(column1, column2),
	)
	if err != nil {
		fmt.Println(err.Error())
		// handle err
	}

	util.FlushLoadCollection(client, "multilingual_documents")

	annSearchParams := index.NewCustomAnnParam()
	annSearchParams.WithExtraParam("metric_type", "BM25")
	annSearchParams.WithExtraParam("analyzer_name", "english")
	annSearchParams.WithExtraParam("drop_ratio_search", 0)

	resultSets, err := client.Search(ctx, milvusclient.NewSearchOption(
		"multilingual_documents", // collectionName
		3,                        // limit
		[]entity.Vector{entity.Text("artificial intelligence")},
	).WithANNSField("sparse").
		WithAnnParam(annSearchParams).
		WithOutputFields("text", "language"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		for i := 0; i < len(resultSet.Scores); i++ {
			text, _ := resultSet.GetColumn("text").GetAsString(i)
			lang, _ := resultSet.GetColumn("language").GetAsString(i)
			fmt.Println("Score: ", resultSet.Scores[i], "Text: ", text, "Language:", lang)
		}
	}

	annSearchParams.WithExtraParam("analyzer_name", "cn")

	resultSets, err = client.Search(ctx, milvusclient.NewSearchOption(
		"multilingual_documents", // collectionName
		3,                        // limit
		[]entity.Vector{entity.Text("人工智能")},
	).WithANNSField("sparse").
		WithAnnParam(annSearchParams).
		WithOutputFields("text", "language"))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	for _, resultSet := range resultSets {
		for i := 0; i < len(resultSet.Scores); i++ {
			text, _ := resultSet.GetColumn("text").GetAsString(i)
			lang, _ := resultSet.GetColumn("language").GetAsString(i)
			fmt.Println("Score: ", resultSet.Scores[i], "Text: ", text, "Language:", lang)
		}
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("multilingual_documents"))
}
