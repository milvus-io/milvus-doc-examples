from pymilvus import MilvusClient, DataType

client = MilvusClient(uri="http://localhost:19530")
client.drop_collection(collection_name="my_varchar_collection")

# define schema
schema = client.create_schema(
    auto_id=False,
    enable_dynamic_fields=True,
)

schema.add_field(field_name="varchar_field1", datatype=DataType.VARCHAR, max_length=100)
schema.add_field(field_name="varchar_field2", datatype=DataType.VARCHAR, max_length=200)
schema.add_field(field_name="pk", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)

# define index
index_params = client.prepare_index_params()

index_params.add_index(
    field_name="varchar_field1",
    index_type="AUTOINDEX",
    index_name="varchar_index"
)

index_params.add_index(
    field_name="embedding",
    index_type="FLAT",
    metric_type="IP",
    params={}
)

# create collection
client.create_collection(
    collection_name="my_varchar_collection",
    schema=schema,
    index_params=index_params
)

# insert
data = [
    {"varchar_field1": "Product A", "varchar_field2": "High quality product", "pk": 1, "embedding": [0.1, 0.2, 0.3]},
    {"varchar_field1": "Product B", "varchar_field2": "Affordable price", "pk": 2, "embedding": [0.4, 0.5, 0.6]},
    {"varchar_field1": "Product C", "varchar_field2": "Best seller", "pk": 3, "embedding": [0.7, 0.8, 0.9]},
]

client.insert(
    collection_name="my_varchar_collection",
    data=data
)

print(client.query(collection_name="my_varchar_collection", filter="", output_fields=["count(*)"], consistency_level="Strong"))

# query
filter = 'varchar_field1 == "Product A"'

res = client.query(
    collection_name="my_varchar_collection",
    filter=filter,
    output_fields=["varchar_field1", "varchar_field2"]
)

print(res)

# search
filter = 'varchar_field1 == "Product A"'

res = client.search(
    collection_name="my_varchar_collection",
    data=[[0.3, -0.6, 0.1]],
    limit=5,
    search_params={"params": {"nprobe": 10}},
    output_fields=["varchar_field1", "varchar_field2"],
    filter=filter
)

print(res)