from pymilvus import MilvusClient

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

client.rename_collection(
    old_name="my_collection",
    new_name="my_new_collection"
)