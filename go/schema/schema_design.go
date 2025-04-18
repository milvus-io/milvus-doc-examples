package schema

import (
	"context"
	"fmt"

	"github.com/milvus-go-examples/util"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func SchemaDesign() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := util.GetClient(ctx)
	if err != nil {
		fmt.Println(err.Error())
	}

	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("article_id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true).
		WithDescription("article id"),
	).WithField(entity.NewField().
		WithName("title").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(200).
		WithDescription("article title"),
	).WithField(entity.NewField().
		WithName("author_info").
		WithDataType(entity.FieldTypeJSON).
		WithDescription("author information"),
	).WithField(entity.NewField().
		WithName("publish_ts").
		WithDataType(entity.FieldTypeInt32).
		WithDescription("publish timestamp"),
	).WithField(entity.NewField().
		WithName("image_url").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(500).
		WithDescription("image url"),
	).WithField(entity.NewField().
		WithName("image_vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(768).
		WithDescription("image vector"),
	).WithField(entity.NewField().
		WithName("summary").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(1000).
		WithDescription("article summary"),
	).WithField(entity.NewField().
		WithName("summary_dense_vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(768).
		WithDescription("summary dense vector"),
	).WithField(entity.NewField().
		WithName("summary_sparse_vector").
		WithDataType(entity.FieldTypeSparseVector).
		WithDescription("summary sparse vector"),
	)

	collectionName := "my_collection"
	indexOption1 := milvusclient.NewCreateIndexOption(collectionName, "image_vector",
		index.NewAutoIndex(index.MetricType(entity.IP)))
	indexOption2 := milvusclient.NewCreateIndexOption(collectionName, "summary_dense_vector",
		index.NewAutoIndex(index.MetricType(entity.IP)))
	// indexOption3 := milvusclient.NewCreateIndexOption(collectionName, "summary_sparse_vector",
	// 	index.NewSparseInvertedIndex(index.MetricType(entity.IP), 0.2))
	indexOption3 := milvusclient.NewCreateIndexOption(collectionName, "summary_sparse_vector",
		index.NewAutoIndex(index.MetricType(entity.IP)))
	indexOption4 := milvusclient.NewCreateIndexOption(collectionName, "publish_ts",
		index.NewInvertedIndex())

	err = client.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption(collectionName, schema).
			WithIndexOptions(indexOption1, indexOption2, indexOption3, indexOption4))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}

	desc, err := client.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collectionName))
	if err != nil {
		fmt.Println(err.Error())
		// handle error
	}
	fmt.Println(desc.Schema)

	client.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
}
