package schema

import (
	"fmt"

	"github.com/milvus-io/milvus/client/v2/entity"
)

func CreateSchema() {
	schema := entity.NewSchema()
	fmt.Println(schema)

	schema.WithField(entity.NewField().WithName("my_id").
		WithDataType(entity.FieldTypeInt64).
		// highlight-start
		WithIsPrimaryKey(true).
		WithIsAutoID(false),
	// highlight-end
	)

	schema.WithField(entity.NewField().WithName("my_vector").
		WithDataType(entity.FieldTypeFloatVector).
		// highlight-next-line
		WithDim(5),
	)

	schema.WithField(entity.NewField().WithName("my_varchar").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(512),
	)

	schema.WithField(entity.NewField().WithName("my_int64").
		WithDataType(entity.FieldTypeInt64),
	)

	schema.WithField(entity.NewField().WithName("my_bool").
		WithDataType(entity.FieldTypeBool),
	)

	schema.WithField(entity.NewField().WithName("my_json").
		WithDataType(entity.FieldTypeJSON),
	)

	schema.WithField(entity.NewField().WithName("my_array").
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeInt64).
		WithMaxLength(512).
		WithMaxCapacity(5),
	)
}

func Int64PrimaryKey() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true).
		WithIsAutoID(true),
	)
}

func VarcharParimaryKey() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_id").
		WithDataType(entity.FieldTypeVarChar).
		// highlight-start
		WithIsPrimaryKey(true).
		WithIsAutoID(true).
		WithMaxLength(512),
	// highlight-end
	)
}
