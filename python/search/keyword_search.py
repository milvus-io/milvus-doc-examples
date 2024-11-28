from pymilvus import MilvusClient, DataType, Function, FunctionType
import random

client = MilvusClient(
    uri="http://localhost:19530",
    token="root:Milvus"
)

def create_schema():
    schema = MilvusClient.create_schema(auto_id=True, enable_dynamic_field=False)

    schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True, auto_id=True)
    schema.add_field(field_name="embeddings", datatype=DataType.FLOAT_VECTOR, dim=128)

    analyzer_params = {
        "type": "english"
    }
    schema.add_field(
        field_name='text',
        datatype=DataType.VARCHAR,
        max_length=200,
        enable_analyzer=True,
        analyzer_params = analyzer_params,
        enable_match = True,
    )
    return schema

def create_index():
    index_params = client.prepare_index_params()

    index_params.add_index(
        field_name="embeddings",
        index_type="FLAT",
        metric_type="L2"
    )
    return index_params

def create_collection(schema, index_params):
    client.drop_collection(collection_name="keyword_search_collection")
    client.create_collection(
        collection_name='keyword_search_collection',
        schema=schema,
        index_params=index_params
    )

def insert():
    client.insert('keyword_search_collection', [
        {'text': 'Artificial intelligence was founded as an academic discipline in 1956.', 'embeddings': [random.random() for _ in range(128)]},
        {'text': 'Alan Turing was the first person to conduct substantial research in AI.', 'embeddings': [random.random() for _ in range(128)]},
        {'text': 'Born in Maida Vale, London, Turing was raised in southern England.', 'embeddings': [random.random() for _ in range(128)]},
    ])
    res = client.query(collection_name="keyword_search_collection",
                       filter="",
                       output_fields=["count(*)"],
                       consistency_level="Strong")
    print(res)

def search(filter: str):
    res = client.search(
        collection_name='keyword_search_collection',
        data=[[random.random() for _ in range(128)]],
        filter=filter,
        anns_field='embeddings',
        limit=3,
        output_fields=["text"],
    )
    for hits in res:
        print("TopK results:")
        for hit in hits:
            print(hit)

def query(filter: str):
    res = client.query(
        collection_name='keyword_search_collection',
        filter=filter,
        output_fields=["id", "text"],
    )
    print("Query results:")
    for hit in res:
        print(hit)

if __name__ == "__main__":
    schema = create_schema()
    index_params = create_index()
    create_collection(schema, index_params)
    insert()

    search("TEXT_MATCH(text, 'Turing London')")
    search("TEXT_MATCH(text, 'Turing') and TEXT_MATCH(text, 'England')")

    query("TEXT_MATCH(text, 'Turing London')")
    query("TEXT_MATCH(text, 'Turing') and TEXT_MATCH(text, 'England')")