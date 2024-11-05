from pymilvus import MilvusClient, DataType

schema = MilvusClient.create_schema()

schema.add_field(
    field_name="my_id",
    datatype=DataType.INT64,
    # highlight-start
    is_primary=True,
    auto_id=True,
    # highlight-end
)

schema.add_field(
    field_name="my_id",
    datatype=DataType.VARCHAR,
    # highlight-start
    is_primary=True,
    auto_id=True,
    max_length=512,
    # highlight-end
)