from pymilvus import MilvusClient, DataType, Function, FunctionType


client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

def create_schema():
    schema = MilvusClient.create_schema(enable_dynamic_field=False)
    schema.add_field(
        field_name="id",
        datatype=DataType.INT64,
        is_primary=True,
        auto_id=True
    )
    analyzer_params = {"type": "english"}
    schema.add_field(
        field_name='text',
        datatype=DataType.VARCHAR,
        max_length=1000,
        enable_analyzer=True,  # Whether to enable text analysis for this field
        analyzer_params=analyzer_params,
        enable_match=True  # Whether to enable text match
    )
    schema.add_field(
        field_name="embeddings",
        datatype=DataType.FLOAT_VECTOR,
        dim=5
    )
    return schema

def create_index():
    index_params = client.prepare_index_params()

    index_params.add_index(
        field_name="embeddings",
        index_type="AUTOINDEX",
        metric_type="L2"
    )
    return index_params

def create_collection(schema, index_params):
    client.drop_collection(collection_name="text_match_collection")
    client.create_collection(
        collection_name='text_match_collection',
        schema=schema,
        index_params=index_params
    )

def insert():
    client.insert('text_match_collection', [
        {'text': 'this is keyword1.', "embeddings": [0.1, 0.2, 0.3, 0.4, 0.5]},
        {'text': 'no keyword.', "embeddings": [0.2, 0.3, 0.4, 0.5, 0.6]},
        {'text': 'this keyword1 and keyword2.', "embeddings": [0.3, 0.4, 0.5, 0.6, 0.7]},
    ])
    client.flush(collection_name="text_match_collection")
    res = client.query(collection_name="text_match_collection",
                       filter="",
                       output_fields=["count(*)"],
                       consistency_level="Strong")
    print(res)

def search(filter: str):
    res = client.search(
        collection_name='text_match_collection',
        data=[[0.1, 0.2, 0.3, 0.5, 0.7]],
        anns_field='embeddings',
        filter=filter,
        limit=3,
        search_params={"params": {"nprobe": 10}},
        output_fields=["id", "text"],
    )
    for hits in res:
        print("TopK results:")
        for hit in hits:
            print(hit)

    result = client.query(
        collection_name="text_match_collection",
        filter=filter,
        output_fields=["id", "text"]
    )
    print(result)

if __name__ == "__main__":
    schema = create_schema()
    index_params = create_index()
    create_collection(schema, index_params)
    insert()

    search("TEXT_MATCH(text, 'keyword1 keyword2')")
    search("TEXT_MATCH(text, 'keyword1') and TEXT_MATCH(text, 'keyword2')")