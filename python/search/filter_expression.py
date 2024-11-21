
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
    schema.add_field("vector", DataType.FLOAT_VECTOR, dim=768)
    schema.add_field(field_name="color", datatype=DataType.VARCHAR, max_length=100)
    schema.add_field("price", DataType.FLOAT)
    schema.add_field("inventory", DataType.JSON)
    schema.add_field("sales_volume", DataType.ARRAY, element_type=DataType.INT64, max_capacity=100)

    index_params = client.prepare_index_params()
    index_params.add_index(field_name="vector",
                           index_type="FLAT",
                           metric_type="IP")

    client.create_collection(collection_name="my_collection",
                             schema=schema,
                             index_params=index_params,
                             consistency_level="Bounded")

    rows = [
        {"id": 1, "vector": [random.random() for _ in range(768)], "color": "pink_8682",
         "price": 593,
         "inventory": {"brand": "Sony", "quantity": 310, "previous_sales": [451, 348, 224]},
         "sales_volume": [161, 81, 51],
         "description": "Sony Xperia 1 VI is a flagship Android smartphone released in 2024 with a 6.5-inch LTPO OLED display"},
        {"id": 2, "vector": [random.random() for _ in range(768)], "color": "red_7025",
         "price": 196,
         "inventory": {"brand": "Samsung", "quantity": 274, "previous_sales": [315, 133, 109]},
         "sales_volume": [126, 126, 125, 96, 155],
         "description": "Galaxy S24 Ultra, Samsung’s latest flagship smartphone."},
        {"id": 3, "vector": [random.random() for _ in range(768)], "color": "orange_6781",
         "price": 862,
         "inventory": {"brand": "Samsung", "quantity": 103, "previous_sales": [232, 254, 275]},
         "sales_volume": [124, 117, 90, 188],
         "description": "Galaxy Fold features the world’s first 7.3-inch Infinity Flex Display."},
        {"id": 4, "vector": [random.random() for _ in range(768)], "color": "pink_9298",
         "price": 991,
         "inventory": {"brand": "Microsoft", "quantity": 175, "previous_sales": [288, 169, 112]},
         "sales_volume": [133, 92, 181, 61, 193],
         "description": "Surface Duo 2, now with lightning-fast 5G(Footnote1) and dynamic triple lens camera."},
        {"id": 5, "vector": [random.random() for _ in range(768)], "color": "red_4794",
         "price": 327,
         "inventory": {"brand": "Apple", "quantity": 193, "previous_sales": [225, 286, 202]},
         "sales_volume": [155, 161, 106, 86, 99],
         "description": "iPhone 15 Pro, A new chip designed for better gaming and other 'pro' features."},
        {"id": 6, "vector": [random.random() for _ in range(768)], "color": "yellow_4222",
         "price": 996,
         "inventory": {"brand": "Microsoft", "quantity": 376, "previous_sales": [254, 275, 232]},
         "sales_volume": [173, 151, 148],
         "description": "The Microsoft Surface Duo seems at first like the perfect little device for this new work-from-home world."},
        {"id": 7, "vector": [random.random() for _ in range(768)], "color": "red_9392",
         "price": 848,
         "inventory": {"brand": "Apple", "quantity": 61, "previous_sales": [312, 254, 367]},
         "sales_volume": [59, 156, 126, 60, 177],
         "description": "The iPhone 14 is a smartphone from Apple Inc. that comes in various colors and sizes."},
        {"id": 8, "vector": [random.random() for _ in range(768)], "color": "grey_8510",
         "price": 241,
         "inventory": {"brand": "Dell", "quantity": 248, "previous_sales": [318, 238, 127]},
         "sales_volume": [105, 126, 114, 132],
         "description": "The Dell Inspiron 15 3000 laptop is equipped with a powerful Intel Core i5-1135G7 Quad-Core Processor, 12GB RAM and 256GB SSD storage."},
        {"id": 9, "vector": [random.random() for _ in range(768)], "color": "white_9381",
         "price": 597,
         "inventory": {"brand": "Apple", "quantity": 351, "previous_sales": [482, 105, 130]},
         "sales_volume": [150, 150, 73],
         "description": "The iPhone 16 features a 6.1-inch OLED display, is powered by Apple's A18 processor, and has dual cameras at the back."},
        {"id": 10, "vector": [random.random() for _ in range(768)], "color": "purple_4976",
         "price": 450,
         "inventory": {"brand": "Apple", "quantity": 268, "previous_sales": [456, 271, 479]},
         "sales_volume": [190, 149, 85, 79, 80],
         "description": "The iPad is a brand of iOS- and iPadOS-based tablet computers that are developed and marketed by Apple."}
    ]
    client.insert(collection_name="my_collection", data=rows)

    res = client.query(collection_name="my_collection",
                       filter="",
                       output_fields=["count(*)"],
                       consistency_level="Strong")
    print(res)


def compare_scalar_query():
    results = client.query(
        collection_name="my_collection",
        filter="500 < price < 900",
        output_fields=["id", "color", "price"]
    )
    for result in results:
        print(result)

def compare_json_query():
    results = client.query(
        collection_name="my_collection",
        filter='inventory["quantity"] >= 250',
        output_fields=["id", "color", "price", "inventory"]
    )
    for result in results:
        print(result)

def compare_array_query():
    results = client.query(
        collection_name="my_collection",
        filter="sales_volume[0] >= 150",
        output_fields=["id", "color", "price", "sales_volume"]
    )
    for result in results:
        print(result)

def condition_scalar_query():
    results = client.query(
        collection_name="my_collection",
        filter='color not in ["red_7025","red_4794","red_9392"]',
        output_fields=["id", "color", "price"]
    )
    for result in results:
        print(result)

def condition_json_query():
    results = client.query(
        collection_name="my_collection",
        filter='inventory["brand"] in ["Apple"]',
        output_fields=["id", "color", "price", "inventory"]
    )
    for result in results:
        print(result)

def match_scalar_query():
    results = client.query(
        collection_name="my_collection",
        filter='color like "red%"',
        output_fields=["id", "color", "price"]
    )
    for result in results:
        print(result)

def match_json_query():
    results = client.query(
        collection_name="my_collection",
        filter='inventory["brand"] like "S%"',
        output_fields=["id", "color", "price", "inventory"]
    )
    for result in results:
        print(result)

def math_scalar_query():
    results = client.query(
        collection_name="my_collection",
        filter="200 <= price*0.5 and price*0.5 <= 300",
        output_fields=["id", "price"]
    )
    for result in results:
        print(result)

def text_match_query():
    results = client.query(
        collection_name="my_collection",
        filter='TEXT_MATCH(description, "Apple iPhone")',
        output_fields=["id", "description"],
    )
    for result in results:
        print(result)

def text_match_query_and():
    results = client.query(
        collection_name="my_collection",
        filter='TEXT_MATCH(description, "chip") and TEXT_MATCH(description, "iPhone")',
        output_fields=["id", "description"],
    )
    for result in results:
        print(result)

def math_json_query():
    results = client.query(
        collection_name="my_collection",
        filter='inventory["quantity"] * 2 > 600',
        output_fields=["id", "color", "price", "inventory"]
    )
    for result in results:
        print(result)

def math_array_query():
    results = client.query(
        collection_name="my_collection",
        filter="sales_volume[0]*2 > 300",
        output_fields=["id", "color", "price", "sales_volume"]
    )
    for result in results:
        print(result)

def json_contains():
    results = client.query(
        collection_name="my_collection",
        filter='JSON_CONTAINS(inventory[\"previous_sales\"], 232)',
        output_fields=["id", "color", "price", "inventory"]
    )
    for result in results:
        print(result)

def json_contains_all():
    results = client.query(
        collection_name="my_collection",
        filter='JSON_CONTAINS_ALL(inventory["previous_sales"], [232, 254, 275])',
        output_fields=["id", "color", "price", "inventory"]
    )
    for result in results:
        print(result)

def json_contains_any():
    results = client.query(
        collection_name="my_collection",
        filter='JSON_CONTAINS_ANY(inventory["previous_sales"], [232, 254, 275])',
        output_fields=["id", "color", "price", "inventory"]
    )
    for result in results:
        print(result)

def array_contains():
    results = client.query(
        collection_name="my_collection",
        filter='ARRAY_CONTAINS(sales_volume, 161)',
        output_fields=["id", "color", "price", "sales_volume"]
    )
    for result in results:
        print(result)

def array_contains_all():
    results = client.query(
        collection_name="my_collection",
        filter='ARRAY_CONTAINS_ALL(sales_volume, [150, 150])',
        output_fields=["id", "color", "price", "sales_volume"]
    )
    for result in results:
        print(result)

def array_contains_any():
    results = client.query(
        collection_name="my_collection",
        filter='ARRAY_CONTAINS_ANY(sales_volume, [150, 190, 90])',
        output_fields=["id", "color", "price", "sales_volume"]
    )
    for result in results:
        print(result)

def array_length():
    results = client.query(
        collection_name="my_collection",
        filter='ARRAY_LENGTH(sales_volume) == 3',
        output_fields=["id", "color", "price", "sales_volume"]
    )
    for result in results:
        print(result)

def multi_filter():
    results = client.query(
        collection_name="my_collection",
        filter='color like "red%" and price < 500 and inventory["brand"] in ["Apple"] and sales_volume[0] > 100',
        output_fields=["id", "color", "price", "inventory", "sales_volume"]
    )
    for result in results:
        print(result)

if __name__ == "__main__":
    create_collection()
    print("================ compare_scalar_query ================")
    compare_scalar_query()
    print("================ compare_json_query ================")
    compare_json_query();
    print("================ compare_array_query ================")
    compare_array_query();
    print("================ condition_scalar_query ================")
    condition_scalar_query();
    print("================ condition_json_query ================")
    condition_json_query();
    print("================ match_scalar_query ================")
    match_scalar_query();
    print("================ match_json_query ================")
    match_json_query();
    print("================ math_scalar_query ================")
    math_scalar_query();
    print("================ math_json_query ================")
    math_json_query();
    print("================ math_array_query ================")
    math_array_query();
    print("================ text_match_query ================")
    text_match_query();
    print("================ text_match_query_and ================")
    text_match_query_and();
    print("================ json_contains ================")
    json_contains();
    print("================ json_contains_all ================")
    json_contains_all();
    print("================ json_contains_any ================")
    json_contains_any();
    print("================ array_contains ================")
    array_contains();
    print("================ array_contains_all ================")
    array_contains_all();
    print("================ array_contains_any ================")
    array_contains_any();
    print("================ array_length ================")
    array_length();
    print("================ multi_filter ================")
    multi_filter();
