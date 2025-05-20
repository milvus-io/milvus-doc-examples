import time

from pymilvus import MilvusClient, DataType, Function, FunctionType
import random


if __name__ == "__main__":
    client = MilvusClient(
        uri="http://localhost:19530",
        token="root:Milvus"
    )

    multi_analyzer_params = {
        # Define language-specific analyzers
        # Each analyzer follows this format: <analyzer_name>: <analyzer_params>
        "analyzers": {
            "english": {"type": "english"},  # English-optimized analyzer
            "chinese": {"type": "chinese"},  # Chinese-optimized analyzer
            "default": {"tokenizer": "icu"}  # Required fallback analyzer
        },
        "by_field": "language",  # Field determining analyzer selection
        "alias": {
            "cn": "chinese",  # Use "cn" as shorthand for Chinese
            "en": "english"  # Use "en" as shorthand for English
        }
    }

    # Initialize a new schema
    schema = client.create_schema()

    # Step 2.1: Add a primary key field for unique document identification
    schema.add_field(
        field_name="id",  # Field name
        datatype=DataType.INT64,  # Integer data type
        is_primary=True,  # Designate as primary key
        auto_id=True  # Auto-generate IDs (recommended)
    )

    # Step 2.2: Add language identifier field
    # This MUST match the "by_field" value in language_analyzer_config
    schema.add_field(
        field_name="language",  # Field name
        datatype=DataType.VARCHAR,  # String data type
        max_length=255  # Maximum length (adjust as needed)
    )

    # Step 2.3: Add text content field with multi-language analysis capability
    schema.add_field(
        field_name="text",  # Field name
        datatype=DataType.VARCHAR,  # String data type
        max_length=8192,  # Maximum length (adjust based on expected text size)
        enable_analyzer=True,  # Enable text analysis
        multi_analyzer_params=multi_analyzer_params  # Connect with our language analyzers
    )

    # Step 2.4: Add sparse vector field to store the BM25 output
    schema.add_field(
        field_name="sparse",  # Field name
        datatype=DataType.SPARSE_FLOAT_VECTOR  # Sparse vector data type
    )

    # Create the BM25 function
    bm25_function = Function(
        name="text_to_vector",  # Descriptive function name
        function_type=FunctionType.BM25,  # Use BM25 algorithm
        input_field_names=["text"],  # Process text from this field
        output_field_names=["sparse"]  # Store vectors in this field
    )

    # Add the function to our schema
    schema.add_function(bm25_function)

    # Configure index parameters
    index_params = client.prepare_index_params()

    # Add index for sparse vector field
    index_params.add_index(
        field_name="sparse",  # Field to index (our vector field)
        index_type="AUTOINDEX",  # Let Milvus choose optimal index type
        metric_type="BM25"  # Must be BM25 for this feature
    )

    # Create collection
    COLLECTION_NAME = "multilingual_documents"

    # Check if collection already exists
    if client.has_collection(COLLECTION_NAME):
        client.drop_collection(COLLECTION_NAME)  # Remove it for this example
        print(f"Dropped existing collection: {COLLECTION_NAME}")

    # Create the collection
    client.create_collection(
        collection_name=COLLECTION_NAME,  # Collection name
        schema=schema,  # Our multilingual schema
        index_params=index_params  # Our search index configuration
    )

    # Prepare multilingual documents
    documents = [
        # English documents
        {
            "text": "Artificial intelligence is transforming technology",
            "language": "english",  # Using full language name
        },
        {
            "text": "Machine learning models require large datasets",
            "language": "en",  # Using our defined alias
        },
        # Chinese documents
        {
            "text": "人工智能正在改变技术领域",
            "language": "chinese",  # Using full language name
        },
        {
            "text": "机器学习模型需要大型数据集",
            "language": "cn",  # Using our defined alias
        },
    ]

    # Insert the documents
    result = client.insert(COLLECTION_NAME, documents)
    client.flush(collection_name=COLLECTION_NAME)
    time.sleep(3)

    # Print results
    inserted = result["insert_count"]
    print(f"Successfully inserted {inserted} documents")
    print("Documents by language: 2 English, 2 Chinese")

    # Expected output:
    # Successfully inserted 4 documents
    # Documents by language: 2 English, 2 Chinese

    search_params = {
        "metric_type": "BM25",  # Must match index configuration
        "analyzer_name": "english",  # Analyzer that matches the query language
        "drop_ratio_search": "0",  # Keep all terms in search (tweak as needed)
    }

    # Execute the search
    english_results = client.search(
        collection_name=COLLECTION_NAME,  # Collection to search
        data=["artificial intelligence"],  # Query text
        anns_field="sparse",  # Field to search against
        search_params=search_params,  # Search configuration
        limit=3,  # Max results to return
        output_fields=["text", "language"],  # Fields to include in the output
        consistency_level="Strong",  # Data‑consistency guarantee
    )

    # Display English search results
    print("\n=== English Search Results ===")
    for i, hit in enumerate(english_results[0]):
        print(f"{i + 1}. [{hit.score:.4f}] {hit.entity.get('text')} "
              f"(Language: {hit.entity.get('language')})")

    # Expected output:
    # === English Search Results ===
    # 1. [2.7881] Artificial intelligence is transforming technology (Language: english)

    search_params["analyzer_name"] = "cn"

    chinese_results = client.search(
        collection_name=COLLECTION_NAME,  # Collection to search
        data=["人工智能"],  # Query text
        anns_field="sparse",  # Field to search against
        search_params=search_params,  # Search configuration
        limit=3,  # Max results to return
        output_fields=["text", "language"],  # Fields to include in the output
        consistency_level="Strong",  # Data‑consistency guarantee
    )

    # Display Chinese search results
    print("\n=== Chinese Search Results ===")
    for i, hit in enumerate(chinese_results[0]):
        print(f"{i + 1}. [{hit.score:.4f}] {hit.entity.get('text')} "
              f"(Language: {hit.entity.get('language')})")

    # Expected output:
    # === Chinese Search Results ===
    # 1. [3.3814] 人工智能正在改变技术领域 (Language: chinese)