from pymilvus import MilvusClient

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

res = client.list_databases()

print(res)

client.create_database(db_name="my_database")

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus",
    db_name="my_database"
)

client.using_database(db_name="my_database")
client.drop_database(db_name="my_database")