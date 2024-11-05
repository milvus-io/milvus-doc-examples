from pymilvus import MilvusClient

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

res = client.list_partitions(
    collection_name="quick_setup"
)

print(res)

client.create_partition(
    collection_name="quick_setup",
    partition_name="partitionA"
)

res = client.list_partitions(
    collection_name="quick_setup"
)

print(res)

res = client.has_partition(
    collection_name="quick_setup",
    partition_name="partitionA"
)

print(res)

client.load_partitions(
    collection_name="quick_setup",
    partition_names=["partitionA"]
)

res = client.get_load_state(
    collection_name="quick_setup",
    partition_name="partitionA"
)

print(res)

client.release_partitions(
    collection_name="quick_setup",
    partition_names=["partitionA"]
)

res = client.get_load_state(
    collection_name="quick_setup",
    partition_name="partitionA"
)

print(res)

client.release_partitions(
    collection_name="quick_setup",
    partition_names=["partitionA"]
)

client.drop_partition(
    collection_name="quick_setup",
    partition_name="partitionA"
)

res = client.list_partitions(
    collection_name="quick_setup"
)

print(res)