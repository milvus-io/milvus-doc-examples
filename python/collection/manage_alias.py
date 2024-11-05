from pymilvus import MilvusClient

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

# 9. Manage aliases
# 9.1. Create aliases
client.create_alias(
    collection_name="customized_setup_2",
    alias="bob"
)

client.create_alias(
    collection_name="customized_setup_2",
    alias="alice"
)

# 9.2. List aliases
res = client.list_aliases(
    collection_name="customized_setup_2"
)

print(res)

# 9.3. Describe aliases
res = client.describe_alias(
    alias="bob"
)

print(res)

# 9.4 Reassign aliases to other collections
client.alter_alias(
    collection_name="customized_setup_1",
    alias="alice"
)

res = client.list_aliases(
    collection_name="customized_setup_1"
)

print(res)

# 9.5 Drop aliases
client.drop_alias(
    alias="bob"
)

client.drop_alias(
    alias="alice"
)