from pymilvus import MilvusClient

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

# delete by filter
res = client.delete(
    collection_name="quick_setup",
    # highlight-next-line
    filter="color in ['red_3314', 'purple_7392']"
)

print(res)

# delete by primary key
res = client.delete(
    collection_name="quick_setup",
    # highlight-next-line
    ids=[18, 19]
)

print(res)

# delete in partition
res = client.delete(
    collection_name="quick_setup",
    ids=[18, 19],
    partition_name="partitionA"
)

print(res)