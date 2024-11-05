from pymilvus import MilvusClient, DataType

client = MilvusClient(uri="http://localhost:19530")
client.drop_collection(collection_name="my_array_collection")

# define schema
schema = client.create_schema(
    auto_id=False,
    enable_dynamic_fields=True,
)

schema.add_field(field_name="tags", datatype=DataType.ARRAY, element_type=DataType.VARCHAR, max_capacity=10, max_length=100)
schema.add_field(field_name="ratings", datatype=DataType.ARRAY, element_type=DataType.INT64, max_capacity=5)
schema.add_field(field_name="pk", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)

# declare index
index_params = client.prepare_index_params()

index_params.add_index(
    field_name="embedding",
    index_type="AUTOINDEX",
    metric_type="L2"
)

index_params.add_index(
    field_name="tags",
    index_type="AUTOINDEX",
    index_name="inverted_index"
)

# create collection
client.create_collection(
    collection_name="my_array_collection",
    schema=schema,
    index_params=index_params
)

# insert
data = [
    {
        "tags": ["pop", "rock", "classic"],
        "ratings": [5, 4, 3],
        "pk": 1,
        "embedding": [0.12, 0.34, 0.56]
    },
    {
        "tags": ["jazz", "blues"],
        "ratings": [4, 5],
        "pk": 2,
        "embedding": [0.78, 0.91, 0.23]
    },
    {
        "tags": ["electronic", "dance"],
        "ratings": [3, 3, 4],
        "pk": 3,
        "embedding": [0.67, 0.45, 0.89]
    }
]

client.insert(
    collection_name="my_array_collection",
    data=data
)

print(client.query(collection_name="my_array_collection", filter="", output_fields=["count(*)"], consistency_level="Strong"))

# query
filter = 'ratings[0] < 4'

res = client.query(
    collection_name="my_array_collection",
    filter=filter,
    output_fields=["tags", "ratings", "embedding"]
)

print(res)

# search
filter = 'tags[0] == "pop"'

res = client.search(
    collection_name="my_array_collection",
    data=[[0.3, -0.6, 0.1]],
    limit=5,
    search_params={"params": {"nprobe": 10}},
    output_fields=["tags", "ratings", "embedding"],
    filter=filter
)

print(res)