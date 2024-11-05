from pymilvus import MilvusClient

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

# 7. Load the collection
client.load_collection(
    collection_name="customized_setup_1"
)

res = client.get_load_state(
    collection_name="customized_setup_1"
)

print(res)

client.release_collection(collection_name="custom_quick_setup")
client.load_collection(
    collection_name="custom_quick_setup",
    # highlight-next-line
    load_fields=["my_id", "my_vector"], # Load only the specified fields
    skip_load_dynamic_field=True # Skip loading the dynamic field
)

res = client.get_load_state(
    collection_name="custom_quick_setup"
)

print(res)

# 8. Release the collection
client.release_collection(
    collection_name="custom_quick_setup"
)

res = client.get_load_state(
    collection_name="custom_quick_setup"
)

print(res)

