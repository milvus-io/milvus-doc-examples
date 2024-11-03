
from pymilvus import MilvusClient, DataType
import random

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)


def create_collection():
    client.drop_collection("my_collection")

    schema = client.create_schema(enable_dynamic_field=True)
    schema.add_field("id", DataType.INT64, is_primary=True, auto_id=False)
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=5)

    index_params = client.prepare_index_params()
    index_params.add_index(field_name="vector",
                           index_type="FLAT",
                           metric_type="IP")

    client.create_collection(collection_name="my_collection",
                             schema=schema,
                             index_params=index_params,
                             consistency_level="Bounded")

    rows = [
        {"id": 1, "vector": [random.random() for _ in range(5)]},
        {"id": 2, "vector": [random.random() for _ in range(5)]},
        {"id": 3, "vector": [random.random() for _ in range(5)]},
        {"id": 4, "vector": [random.random() for _ in range(5)]},
        {"id": 5, "vector": [random.random() for _ in range(5)]},
    ]
    client.insert(collection_name="my_collection", data=rows)

    client.create_partition(collection_name="my_collection", partition_name="partitionA")
    rows = [
        {"id": 6, "vector": [random.random() for _ in range(5)]},
        {"id": 7, "vector": [random.random() for _ in range(5)]},
        {"id": 8, "vector": [random.random() for _ in range(5)]},
        {"id": 9, "vector": [random.random() for _ in range(5)]},
        {"id": 10, "vector": [random.random() for _ in range(5)]},
    ]
    client.insert(collection_name="my_collection", partition_name="partitionA", data=rows)

    res = client.query(collection_name="my_collection",
                       filter="",
                       output_fields=["count(*)"],
                       consistency_level="Strong")
    print(res)


def single_vector_search():
    query_vector = [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592]
    res = client.search(
        collection_name="my_collection",
        anns_field="vector",
        data=[query_vector],
        limit=3,
    )

    for hits in res:
        print("TopK results:")
        for hit in hits:
            print(hit)


def batch_vectors_search():
    query_vectors = [
        [0.041732933, 0.013779674, -0.027564144, -0.013061441, 0.009748648],
        [0.0039737443, 0.003020432, -0.0006188639, 0.03913546, -0.00089768134]
    ]

    res = client.search(
        collection_name="my_collection",
        data=query_vectors,
        limit=3,
    )

    for hits in res:
        print("TopK results:")
        for hit in hits:
            print(hit)


def search_in_partition():
    query_vector = [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592]
    res = client.search(
        collection_name="my_collection",
        # highlight-next-line
        partition_names=["partitionA"],
        data=[query_vector],
        limit=3,
    )

    for hits in res:
        print("TopK results:")
        for hit in hits:
            print(hit)


if __name__ == "__main__":
    create_collection()
    print("================ single_vector_search ================")
    single_vector_search()
    print("================ batch_vectors_search ================")
    batch_vectors_search()
    print("================ search_in_partition ================")
    search_in_partition()
