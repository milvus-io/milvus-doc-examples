from pymilvus import MilvusClient, DataType

client = MilvusClient(uri="http://localhost:19530")

client.drop_collection(collection_name="my_dense_collection")

# define dense vector field
schema = client.create_schema(
    auto_id=True,
    enable_dynamic_fields=True,
)

schema.add_field(field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100)
schema.add_field(field_name="dense_vector", datatype=DataType.FLOAT_VECTOR, dim=4)

# define index for dense vector
index_params = client.prepare_index_params()

index_params.add_index(
    field_name="dense_vector",
    index_name="dense_vector_index",
    index_type="IVF_FLAT",
    metric_type="IP",
    params={"nlist": 128}
)

# create collection
client.create_collection(
    collection_name="my_dense_collection",
    schema=schema,
    index_params=index_params
)

# insert
data = [
    {"dense_vector": [0.1, 0.2, 0.3, 0.7]},
    {"dense_vector": [0.2, 0.3, 0.4, 0.8]},
]

client.insert(
    collection_name="my_dense_collection",
    data=data
)

print(client.query(collection_name="my_dense_collection", filter="", output_fields=["count(*)"], consistency_level="Strong"))

# search
search_params = {
    "params": {"nprobe": 10}
}

query_vector = [0.1, 0.2, 0.3, 0.7]

res = client.search(
    collection_name="my_dense_collection",
    data=[query_vector],
    anns_field="dense_vector",
    search_params=search_params,
    limit=5,
    output_fields=["pk"]
)

print(res)