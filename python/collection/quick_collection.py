from pymilvus import MilvusClient, DataType

CLUSTER_ENDPOINT = "http://localhost:19530"
TOKEN = "root:Milvus"

# 1. Set up a Milvus client
client = MilvusClient(
    uri=CLUSTER_ENDPOINT,
    token=TOKEN
)

def create_simple():
    # 2. Create a collection in quick setup mode
    client.create_collection(
        collection_name="quick_setup",
        dimension=5
    )

    res = client.get_load_state(
        collection_name="quick_setup"
    )

    print(res)

def create_custom():
    # 2. Create a collection in quick setup mode
    client.create_collection(
        collection_name="custom_quick_setup",
        dimension=5,
        primary_field_name="my_id",
        id_type="string",
        vector_field_name="my_vector",
        metric_type="L2",
        auto_id=True,
        max_length=512
    )

    res = client.get_load_state(
        collection_name="custom_quick_setup"
    )

    print(res)

if __name__ == "__main__":
    create_simple()
    create_custom()