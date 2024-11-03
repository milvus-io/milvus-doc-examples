# Create a collection in customized setup mode
from pymilvus import (
    MilvusClient, DataType, AnnSearchRequest, WeightedRanker, RRFRanker
)

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)


def create_collection():
    # Create schema
    schema = MilvusClient.create_schema(
        auto_id=False,
        enable_dynamic_field=True,
    )
    # Add fields to schema
    schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name="vector", datatype=DataType.FLOAT_VECTOR, dim=5)
    schema.add_field(field_name="color", datatype=DataType.VARCHAR, max_length=1000)

    # Prepare index parameters
    index_params = client.prepare_index_params()

    # Add indexes
    index_params.add_index(
        field_name="vector",
        index_type="IVF_FLAT",
        metric_type="L2",
        params={"nlist": 128},
    )

    client.drop_collection(collection_name="query_collection")
    client.create_collection(
        collection_name="query_collection",
        schema=schema,
        index_params=index_params
    )

    data=[
        {"id": 0, "vector": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592], "color": "pink_8682"},
        {"id": 1, "vector": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104], "color": "red_7025"},
        {"id": 2, "vector": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185, 0.20785793220625592], "color": "orange_6781"},
        {"id": 3, "vector": [0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995, 0.95791889146345], "color": "pink_9298"},
        {"id": 4, "vector": [0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184, 0.30337481143159106], "color": "red_4794"},
        {"id": 5, "vector": [0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383, -0.1446277761879955], "color": "yellow_4222"},
        {"id": 6, "vector": [0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192, -0.8984947637863987], "color": "red_9392"},
        {"id": 7, "vector": [-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709, 0.5378064918413052], "color": "grey_8510"},
        {"id": 8, "vector": [0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872, -0.6140360785406336], "color": "white_9381"},
        {"id": 9, "vector": [0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717, -0.6980531615588608], "color": "purple_4976"},
    ]
    res = client.insert(
        collection_name="query_collection",
        data=data
    )

    client.create_partition(collection_name="query_collection", partition_name="partitionA")
    data = [
        {"id": 10, "vector": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354,
                             0.9029438446296592], "color": "pink_8682"},
        {"id": 11, "vector": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501,
                             0.838729485096104], "color": "red_7025"},
        {"id": 12, "vector": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.7894058910881185,
                             0.20785793220625592], "color": "orange_6781"},
        {"id": 13, "vector": [0.3172005263489739, 0.9719044792798428, -0.36981146090600725, -0.4860894583077995,
                             0.95791889146345], "color": "pink_9298"},
        {"id": 14, "vector": [0.4452349528804562, -0.8757026943054742, 0.8220779437047674, 0.46406290649483184,
                             0.30337481143159106], "color": "red_4794"},
        {"id": 15, "vector": [0.985825131989184, -0.8144651566660419, 0.6299267002202009, 0.1206906911183383,
                             -0.1446277761879955], "color": "yellow_4222"},
        {"id": 16, "vector": [0.8371977790571115, -0.015764369584852833, -0.31062937026679327, -0.562666951622192,
                             -0.8984947637863987], "color": "red_9392"},
        {"id": 17, "vector": [-0.33445148015177995, -0.2567135004164067, 0.8987539745369246, 0.9402995886420709,
                             0.5378064918413052], "color": "grey_8510"},
        {"id": 18, "vector": [0.39524717779832685, 0.4000257286739164, -0.5890507376891594, -0.8650502298996872,
                             -0.6140360785406336], "color": "white_9381"},
        {"id": 19, "vector": [0.5718280481994695, 0.24070317428066512, -0.3737913482606834, -0.06726932177492717,
                             -0.6980531615588608], "color": "purple_4976"},
    ]
    res = client.insert(
        collection_name="query_collection",
        partition_name="partitionA",
        data=data
    )
    res = client.query(collection_name="query_collection",
                       filter="",
                       output_fields=["count(*)"],
                       consistency_level="Strong")
    print(res)


def get():
    res = client.get(
        collection_name="query_collection",
        ids=[0, 1, 2],
        output_fields=["vector", "color"]
    )
    print(res)


def query():
    res = client.query(
        collection_name="query_collection",
        filter="color like \"red%\"",
        output_fields=["vector", "color"],
        limit=3
    )
    print(res)


def query_iterator():
    from pymilvus import connections, Collection

    connections.connect(
        uri="http://localhost:19530",
        token="root:Milvus"
    )

    collection = Collection("query_collection")

    iterator = collection.query_iterator(
        batch_size=10,
        expr="color like \"red%\"",
        output_fields=["color"]
    )

    results = []

    while True:
        result = iterator.next()
        if not result:
            iterator.close()
            break

        print(result)
        results += result


def query_in_partition():
    from pymilvus import MilvusClient
    client = MilvusClient(
        uri="http://localhost:19530",
        token="root:Milvus"
    )

    res = client.get(
        collection_name="query_collection",
        # highlight-next-line
        partitionNames=["partitionA"],
        ids=[0, 1, 2],
        output_fields=["vector", "color"]
    )

    from pymilvus import MilvusClient

    client = MilvusClient(
        uri="http://localhost:19530",
        token="root:Milvus"
    )

    res = client.query(
        collection_name="query_collection",
        # highlight-next-line
        partitionNames=["partitionA"],
        filter="color like \"red%\"",
        output_fields=["vector", "color"],
        limit=3
    )

    # 使用 QueryIterator
    from pymilvus import connections, Collection

    connections.connect(
        uri="http://localhost:19530",
        token="root:Milvus"
    )

    collection = Collection("query_collection")

    iterator = collection.query_iterator(
        # highlight-next-line
        partition_names=["partitionA"],
        batch_size=10,
        expr="color like \"red%\"",
        output_fields=["color"]
    )

    results = []

    while True:
        result = iterator.next()
        if not result:
            iterator.close()
            break

        print(result)
        results += result


if __name__ == "__main__":
    create_collection()
    print("================ get ================")
    get()
    print("================ query ================")
    query()
    print("================ query_iterator ================")
    query_iterator()
    print("================ query_in_partition ================")
    query_in_partition()


