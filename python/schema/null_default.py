from pymilvus import MilvusClient, DataType

client = MilvusClient(uri='http://localhost:19530')
client.drop_collection(collection_name="user_profiles_null")

# define collection schema
schema = client.create_schema(
    auto_id=False,
    enable_dynamic_schema=True,
)

schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=5)
schema.add_field(field_name="age", datatype=DataType.INT64, nullable=True) # Nullable field

# set index params
index_params = client.prepare_index_params()
index_params.add_index(field_name="vector", index_type="IVF_FLAT", metric_type="L2", params={ "nlist": 128 })

# create collection with null value
client.create_collection(collection_name="user_profiles_null", schema=schema, index_params=index_params)

# insert
data = [
    {"id": 1, "vector": [0.1, 0.2, 0.3, 0.4, 0.5], "age": 30},
    {"id": 2, "vector": [0.2, 0.3, 0.4, 0.5, 0.6], "age": None},
    {"id": 3, "vector": [0.3, 0.4, 0.5, 0.6, 0.7]}
]

client.insert(collection_name="user_profiles_null", data=data)

print(client.query(collection_name="user_profiles_null", filter="", output_fields=["count(*)"], consistency_level="Strong"))

# search
res = client.search(
    collection_name="user_profiles_null",
    data=[[0.1, 0.2, 0.4, 0.3, 0.128]],
    limit=2,
    search_params={"params": {"nprobe": 16}},
    output_fields=["id", "age"]
)

print(res)


# create collection with default value
client.drop_collection(collection_name="user_profiles_default")

schema = client.create_schema(
    auto_id=False,
    enable_dynamic_schema=True,
)

schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=5)
schema.add_field(field_name="age", datatype=DataType.INT64, default_value=18)
schema.add_field(field_name="status", datatype=DataType.VARCHAR, default_value="active", max_length=10)

index_params = client.prepare_index_params()
index_params.add_index(field_name="vector", index_type="IVF_FLAT", metric_type="L2", params={ "nlist": 128 })

client.create_collection(collection_name="user_profiles_default", schema=schema, index_params=index_params)

# insert
data = [
    {"id": 1, "vector": [0.1, 0.2, 0.3, 0.4, 0.5], "age": 30, "status": "premium"},
    {"id": 2, "vector": [0.2, 0.3, 0.4, 0.5, 0.6]},
    {"id": 3, "vector": [0.3, 0.4, 0.5, 0.6, 0.7], "age": 25, "status": None},
    {"id": 4, "vector": [0.4, 0.5, 0.6, 0.7, 0.8], "age": None, "status": "inactive"}
]

client.insert(collection_name="user_profiles_default", data=data)

print(client.query(collection_name="user_profiles_default", filter="", output_fields=["count(*)"], consistency_level="Strong"))

# search
res = client.search(
    collection_name="user_profiles_default",
    data=[[0.1, 0.2, 0.4, 0.3, 0.128]],
    search_params={"params": {"nprobe": 16}},
    filter="age == 18",  # 18 is the default value of the `age` field
    limit=10,
    output_fields=["id", "age", "status"]
)

print(res)