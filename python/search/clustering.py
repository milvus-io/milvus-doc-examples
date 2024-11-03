from pymilvus import MilvusClient, DataType

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

schema = client.create_schema()
schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=5)

client.drop_collection(collection_name="my_collection")
client.create_collection(
    collection_name="my_collection",
    schema=schema,
    clustering_key_field=True,
)

from pymilvus import connections, Collection

connections.connect(
    uri="http://localhost:19530",
    token="root:Milvus"
)

collection = Collection("my_collection")

collection.compact(is_clustering=True)

if collection.wait_for_compaction_completed(is_clustering=True):
    collection.get_compaction_state(is_clustering=True)