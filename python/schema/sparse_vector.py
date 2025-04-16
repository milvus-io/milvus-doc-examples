from scipy.sparse import csr_matrix

# declare sparse vectors by scipy.sparse
row = [0, 0, 1, 2, 2, 2]
col = [0, 2, 2, 0, 1, 2]
data = [1, 2, 3, 4, 5, 6]
sparse_matrix = csr_matrix((data, (row, col)), shape=(3, 3))

sparse_vector = sparse_matrix.getrow(0)

# declare sparse vectors by dict
sparse_vector = [{1: 0.5, 100: 0.3, 500: 0.8, 1024: 0.2, 5000: 0.6}]

# declare sparse vectors by tuple
sparse_vector = [[(1, 0.5), (100, 0.3), (500, 0.8), (1024, 0.2), (5000, 0.6)]]

# define sparse vector field
from pymilvus import MilvusClient, DataType

client = MilvusClient(uri="http://localhost:19530")

client.drop_collection(collection_name="my_sparse_collection")

schema = client.create_schema(
    auto_id=True,
    enable_dynamic_fields=True,
)

schema.add_field(field_name="pk", datatype=DataType.VARCHAR, is_primary=True, max_length=100)
schema.add_field(field_name="sparse_vector", datatype=DataType.SPARSE_FLOAT_VECTOR)


# define index of sparse vector
index_params = client.prepare_index_params()

index_params.add_index(
    field_name="sparse_vector",
    index_name="sparse_inverted_index",
    index_type="SPARSE_INVERTED_INDEX",
    metric_type="IP",
    params={"inverted_index_algo": "DAAT_MAXSCORE"},
)

# create collection
client.create_collection(
    collection_name="my_sparse_collection",
    schema=schema,
    index_params=index_params
)

# insert
sparse_vectors = [
    {"sparse_vector": {1: 0.5, 100: 0.3, 500: 0.8}},
    {"sparse_vector": {10: 0.1, 200: 0.7, 1000: 0.9}},
]

client.insert(
    collection_name="my_sparse_collection",
    data=sparse_vectors
)

print(client.query(collection_name="my_sparse_collection", filter="", output_fields=["count(*)"], consistency_level="Strong"))

# search
search_params = {
    "params": {"drop_ratio_search": 0.2},
}

query_vector = [{1: 0.2, 50: 0.4, 1000: 0.7}]

res = client.search(
    collection_name="my_sparse_collection",
    data=query_vector,
    limit=3,
    output_fields=["pk"],
    search_params=search_params,
)

print(res)