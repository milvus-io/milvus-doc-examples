# Create a collection in customized setup mode
from pymilvus import (
    MilvusClient, DataType, AnnSearchRequest, WeightedRanker, RRFRanker
)

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)



def create_schema():
    # Create schema
    schema = MilvusClient.create_schema(
        auto_id=False,
        enable_dynamic_field=True,
    )
    # Add fields to schema
    schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
    schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=1000)
    schema.add_field(field_name="sparse", datatype=DataType.SPARSE_FLOAT_VECTOR)
    schema.add_field(field_name="dense", datatype=DataType.FLOAT_VECTOR, dim=5)
    return schema

def create_index():
    # Prepare index parameters
    index_params = client.prepare_index_params()

    # Add indexes
    index_params.add_index(
        field_name="dense",
        index_name="dense_index",
        index_type="IVF_FLAT",
        metric_type="IP",
        params={"nlist": 128},
    )

    index_params.add_index(
        field_name="sparse",
        index_name="sparse_index",
        index_type="SPARSE_INVERTED_INDEX",  # Index type for sparse vectors
        metric_type="IP",  # Currently, only IP (Inner Product) is supported for sparse vectors
        params={"drop_ratio_build": 0.2},  # The ratio of small vector values to be dropped during indexing
    )
    return index_params


def create_collection(schema, index_params):
    client.drop_collection(collection_name="hybrid_search_collection")
    client.create_collection(
        collection_name="hybrid_search_collection",
        schema=schema,
        index_params=index_params
    )


def insert():
    data=[
        {"id": 0, "text": "Artificial intelligence was founded as an academic discipline in 1956.", "sparse": {9637: 0.30856525997853057, 4399: 0.19771651149001523}, "dense": [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.4860894583077995, 0.95791889146345]},
        {"id": 1, "text": "Alan Turing was the first person to conduct substantial research in AI.", "sparse": {6959: 0.31025067641541815, 1729: 0.8265339135915016}, "dense": [0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.1206906911183383, -0.1446277761879955]},
        {"id": 2, "text": "Born in Maida Vale, London, Turing was raised in southern England.", "sparse": {1220: 0.15303302147479103, 7335: 0.9436728846033107}, "dense": [0.43742130801983836, -0.5597502546264526, 0.6457887650909682, 0.9402995886420709, 0.5378064918413052]},
    ]
    res = client.insert(
        collection_name="hybrid_search_collection",
        data=data
    )
    res = client.query(collection_name="hybrid_search_collection",
                       filter="",
                       output_fields=["count(*)"],
                       consistency_level="Strong")
    print(res)


def declare_request():
    query_dense_vector = [0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592]

    search_param_1 = {
        "data": [query_dense_vector],
        "anns_field": "dense",
        "param": {
            "metric_type": "IP",
            "params": {"nprobe": 10}
        },
        "limit": 2
    }
    request_1 = AnnSearchRequest(**search_param_1)

    query_sparse_vector = {3573: 0.34701499565746674}, {5263: 0.2639375518635271}
    search_param_2 = {
        "data": [query_sparse_vector],
        "anns_field": "sparse",
        "param": {
            "metric_type": "IP",
            "params": {"drop_ratio_build": 0.2}
        },
        "limit": 2
    }
    request_2 = AnnSearchRequest(**search_param_2)

    reqs = [request_1, request_2]
    return reqs


def hybrid_search(reqs, ranker):
    res = client.hybrid_search(
        collection_name="hybrid_search_collection",
        reqs=reqs,
        ranker=ranker,
        limit=2
    )
    for hits in res:
        print("TopK results:")
        for hit in hits:
            print(hit)


if __name__ == "__main__":
    schema = create_schema()
    index_params = create_index()
    create_collection(schema, index_params)
    insert()
    reqs = declare_request()

    ranker = WeightedRanker(0.8, 0.3)
    # ranker = RRFRanker(100)
    hybrid_search(reqs, ranker)
