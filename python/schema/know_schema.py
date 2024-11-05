from pymilvus import MilvusClient, DataType

schema = MilvusClient.create_schema()

schema.add_field(
    field_name="my_id",
    datatype=DataType.INT64,
    # highlight-start
    is_primary=True,
    auto_id=False,
    # highlight-end
)

schema.add_field(
    field_name="my_vector",
    datatype=DataType.FLOAT_VECTOR,
    # highlight-next-line
    dim=5
)

schema.add_field(
    field_name="my_varchar",
    datatype=DataType.VARCHAR,
    # highlight-next-line
    max_length=512
)

schema.add_field(
    field_name="my_int64",
    datatype=DataType.INT64,
)

schema.add_field(
    field_name="my_bool",
    datatype=DataType.BOOL,
)

schema.add_field(
    field_name="my_json",
    datatype=DataType.JSON,
)

schema.add_field(
    field_name="my_array",
    datatype=DataType.ARRAY,
    element_type=DataType.VARCHAR,
    max_capacity=5,
    max_length=512,
)