from pymilvus import MilvusClient, DataType

client = MilvusClient(uri="http://localhost:19530")
client.drop_collection(collection_name="my_scalar_collection")

# define schema
schema = client.create_schema(
    auto_id=False,
    enable_dynamic_fields=True,
)

schema.add_field(field_name="age", datatype=DataType.INT64)
schema.add_field(field_name="price", datatype=DataType.FLOAT)
schema.add_field(field_name="pk", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)

# define indexes
index_params = client.prepare_index_params()

index_params.add_index(
    field_name="age",
    index_type="AUTOINDEX",
    index_name="inverted_index"
)

index_params.add_index(
    field_name="embedding",
    index_type="FLAT",
    metric_type="IP",
    params={}
)

# create collection
client.create_collection(
    collection_name="my_scalar_collection",
    schema=schema,
    index_params=index_params
)

# insert
data = [
    {"age": 25, "price": 99.99, "pk": 1, "embedding": [0.1, 0.2, 0.3]},
    {"age": 30, "price": 149.50, "pk": 2, "embedding": [0.4, 0.5, 0.6]},
    {"age": 35, "price": 199.99, "pk": 3, "embedding": [0.7, 0.8, 0.9]},
]

client.insert(
    collection_name="my_scalar_collection",
    data=data
)

print(client.query(collection_name="my_scalar_collection", filter="", output_fields=["count(*)"], consistency_level="Strong"))

# query
filter = "30 <= age <= 40"

res = client.query(
    collection_name="my_scalar_collection",
    filter=filter,
    output_fields=["age","price"]
)

print(res)

# search
filter = "25 <= age <= 35"

res = client.search(
    collection_name="my_scalar_collection",
    data=[[0.3, -0.6, 0.1]],
    limit=5,
    search_params={"params": {"nprobe": 10}},
    output_fields=["age","price"],
    filter=filter
)

print(res)