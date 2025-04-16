package schema

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func AnalyzerOverview() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer client.Close(ctx)

	analyzerParams := map[string]any{"type": "standard", "stop_words": []string{"a", "an", "for"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard",
		"filter": []any{"lowercase", map[string]any{
			"type":       "stop",
			"stop_words": []string{"a", "an", "for"},
		}}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "whitespace"}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard",
		"filter": []any{"lowercase"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard",
		"filter": []any{map[string]any{
			"type":       "stop",
			"stop_words": []string{"of", "to"},
		}}}
	fmt.Println(analyzerParams)

	schema := entity.NewSchema().WithAutoID(true).WithDynamicFieldEnabled(true)
	fmt.Println(schema)

	schema.WithField(entity.NewField().
		WithName("id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true).
		WithIsAutoID(true),
	).WithField(entity.NewField().
		WithName("embedding").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(3),
	).WithField(entity.NewField().
		WithName("title").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(1000).
		WithEnableAnalyzer(true).
		WithAnalyzerParams(analyzerParams).
		WithEnableMatch(true),
	)

	idx := index.NewAutoIndex(index.MetricType(entity.COSINE))
	indexOption := milvusclient.NewCreateIndexOption("YOUR_COLLECTION_NAME", "embedding", idx)

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption("YOUR_COLLECTION_NAME", schema).
			WithIndexOptions(indexOption))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption("YOUR_COLLECTION_NAME"))
}

func AnalyzerBuiltin() {
	analyzerParams := map[string]any{"tokenizer": "standard", "filter": []any{"lowercase"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"type": "standard"}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"type": "standard", "stop_words": []string{"of"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"type": "standard", "stop_words": []string{"for"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard",
		"filter": []any{"lowercase", map[string]any{
			"type":     "stemmer",
			"language": "english",
		}, map[string]any{
			"type":       "stop",
			"stop_words": "_english_",
		}}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"type": "english"}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"type": "english", "stop_words": []string{"a", "an", "the"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "jieba", "filter": []any{"cnalphanumonly"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"type": "chinese"}
	fmt.Println(analyzerParams)

}

func Tokenizer() {
	analyzerParams := map[string]any{"tokenizer": "standard"}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard", "filter": []any{"lowercase"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "whitespace"}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "whitespace", "filter": []any{"lowercase"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "jieba"}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"type": "jieba", "dict": []any{"_default_"}, "mode": "search", "hmm": true}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"type": "jieba", "dict": []any{"customDictionary"}, "mode": "exact", "hmm": false}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"type": "jieba", "dict": []any{"结巴分词器"}, "mode": "exact", "hmm": false}
	fmt.Println(analyzerParams)
}

func Filter() {
	analyzerParams := map[string]any{"tokenizer": "standard", "filter": []any{"lowercase"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard", "filter": []any{"asciifolding"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard", "filter": []any{"alphanumonly"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard", "filter": []any{"cnalphanumonly"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard", "filter": []any{"cncharonly"}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard",
		"filter": []any{map[string]any{
			"type": "length",
			"max":  10,
		}}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard",
		"filter": []any{map[string]any{
			"type":       "stop",
			"stop_words": []string{"of", "to", "_english_"},
		}}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard",
		"filter": []any{map[string]any{
			"type":      "decompounder",
			"word_list": []string{"dampf", "schiff", "fahrt", "brot", "backen", "automat"},
		}}}
	fmt.Println(analyzerParams)

	analyzerParams = map[string]any{"tokenizer": "standard",
		"filter": []any{map[string]any{
			"type":     "stemmer",
			"language": "english",
		}}}
	fmt.Println(analyzerParams)
}
