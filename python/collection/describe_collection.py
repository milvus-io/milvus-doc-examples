from pymilvus import MilvusClient, DataType

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

res = client.list_collections()

print(res)

res = client.describe_collection(
    collection_name="quick_setup"
)

print(res)