from pymilvus import MilvusClient, DataType, Function, FunctionType


client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

def create_schema():
    schema = client.create_schema()

    schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True, auto_id=True)
    schema.add_field(field_name="text", datatype=DataType.VARCHAR, max_length=1000, enable_analyzer=True)
    schema.add_field(field_name="sparse", datatype=DataType.SPARSE_FLOAT_VECTOR)

    bm25_function = Function(
        name="text_bm25_emb",  # Function name
        input_field_names=["text"],  # Name of the VARCHAR field containing raw text data
        output_field_names=["sparse"],
        # Name of the SPARSE_FLOAT_VECTOR field reserved to store generated embeddings
        function_type=FunctionType.BM25,
    )

    schema.add_function(bm25_function)
    return schema

def create_index():
    index_params = client.prepare_index_params()

    index_params.add_index(
        field_name="sparse",
        index_type="AUTOINDEX",
        metric_type="BM25"
    )
    return index_params

def create_collection(schema, index_params):
    client.drop_collection(collection_name="full_text_search_collection")
    client.create_collection(
        collection_name='full_text_search_collection',
        schema=schema,
        index_params=index_params
    )

def insert():
    client.insert('full_text_search_collection', [
        {'text': 'Artificial intelligence was founded as an academic discipline in 1956.'},
        {'text': 'Alan Turing was the first person to conduct substantial research in AI.'},
        {'text': 'Born in Maida Vale, London, Turing was raised in southern England.'},
    ])
    res = client.query(collection_name="full_text_search_collection",
                       filter="",
                       output_fields=["count(*)"],
                       consistency_level="Strong")
    print(res)

def search(text: str):
    search_params = {
        'params': {'drop_ratio_search': 0.2},
    }

    res = client.search(
        collection_name='full_text_search_collection',
        data=[text],
        anns_field='sparse',
        limit=3,
        output_fields=["text"],
        search_params=search_params,
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

    search('When Artificial intelligence was founded?')
    search('Who started AI research?')