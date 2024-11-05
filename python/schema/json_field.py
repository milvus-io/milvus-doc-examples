from pymilvus import MilvusClient, DataType

client = MilvusClient(uri="http://localhost:19530")
client.drop_collection(collection_name="my_json_collection")

# define schema
schema = client.create_schema(
    auto_id=False,
    enable_dynamic_fields=True,
)

schema.add_field(field_name="metadata", datatype=DataType.JSON)
schema.add_field(field_name="pk", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)

# declare index
index_params = client.prepare_index_params()

index_params.add_index(
    field_name="embedding",
    index_type="AUTOINDEX",
    metric_type="COSINE"
)

# create collection
client.create_collection(
    collection_name="my_json_collection",
    schema=schema,
    index_params=index_params
)

# insert data
data = [
  {
      "metadata": {"category": "electronics", "price": 99.99, "brand": "BrandA"},
      "pk": 1,
      "embedding": [0.12, 0.34, 0.56]
  },
  {
      "metadata": {"category": "home_appliances", "price": 249.99, "brand": "BrandB"},
      "pk": 2,
      "embedding": [0.56, 0.78, 0.90]
  },
  {
      "metadata": {"category": "furniture", "price": 399.99, "brand": "BrandC"},
      "pk": 3,
      "embedding": [0.91, 0.18, 0.23]
  }
]

client.insert(
    collection_name="my_json_collection",
    data=data
)

print(client.query(collection_name="my_json_collection", filter="", output_fields=["count(*)"], consistency_level="Strong"))

# query
filter = 'metadata["category"] == "electronics" and metadata["price"] < 150'

res = client.query(
    collection_name="my_json_collection",
    filter=filter,
    output_fields=["metadata"]
)

print(res)

# search
filter = 'metadata["brand"] == "BrandA"'

res = client.search(
    collection_name="my_json_collection",
    data=[[0.3, -0.6, 0.1]],
    limit=5,
    search_params={"params": {"nprobe": 10}},
    output_fields=["metadata"],
    filter=filter
)

print(res)