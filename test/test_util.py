import unittest
from elasticsearch import Elasticsearch
from time import sleep
from util import *
from decimal import *


ELASTIC_HOST = "http://localhost:9200"
PRODUCT_INDEX = "products"

INDEX_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "analysis": {
            "normalizer": {
                "use_lowercase": {
                    "type": "custom",
                    "filter": ["lowercase"]
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "brand": {
                "type":  "keyword",
                "normalizer": "use_lowercase"
            },
            "relation": {
                "type": "join",
                "relations": {
                    "origin": "product"
                }
            }
        }
    }
}
INDEX_SETTINGS = {
    "settings": {
        "index": {
            "analysis": {
                "normalizer": {
                    "use_lowercase": {
                        "type": "custom",
                        "filter": ["lowercase"]
                    }
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "normalizedName": {
                "type": "text",
            },
            "brand": {
                "type":  "keyword",
                "normalizer": "use_lowercase"
            },
            "relation": {
                "type": "join",
                "relations": {
                    "origin": "product"
                }
            }
        }
    }
}


def get_es_instance():
    elasticsearch = Elasticsearch(ELASTIC_HOST, verify_certs=False)
    for _ in range(20):
        sleep(1)
        try:
            elasticsearch.cluster.health(wait_for_status='yellow')
            return elasticsearch
        except Exception as err:
            print(err)
            continue
    else:
        raise RuntimeError("Elasticsearch failed to start.")


class TestUtil(unittest.TestCase):

    es = get_es_instance()

    def setUp(self):
        if not self.es.indices.exists(index=PRODUCT_INDEX):
            self.es.indices.create(PRODUCT_INDEX, body=INDEX_SETTINGS)

    def tearDown(self):
        self.es.indices.delete(index=PRODUCT_INDEX, ignore=[400, 404])

    def test_find_origin(self):
        self.es.index(index=PRODUCT_INDEX, refresh=True, routing="1", id="grivel G14",
                      body={"relation": {"name": "origin"}, "name": "G14", "normalizedName": "grivel G14", "brand": "grivel", "isCanonical": True})
        actual_origin = find_origin(self.es, PRODUCT_INDEX, "G14", "grivel")
        print(actual_origin)
        assert actual_origin == "G14"

    def test_product_record_with_origin(self):
        self.es.index(index=PRODUCT_INDEX, refresh=True, routing="1", id="lynx",
                      body={"relation": {"name": "origin"}, "name": "lynx", "brand": "petzl", "isCanonical": True})
        product = {
            "url": "http://lynx.com",
            "name": "petzl lynx crampons",
            "brand": "petzl",
            "currency": "USD",
            "price": 99.90,
            "store": "www.store.com",
            "imageUrl": "http://image.com"
        }
        actual_records = process_product(self.es, PRODUCT_INDEX, product)
        expected_records = ProductOriginPair(
            None,
            {
                '_index': PRODUCT_INDEX, '_id': 'http://lynx.com', '_routing': 'lynx', 'url': 'http://lynx.com',
                'store': 'www.store.com', 'name': 'petzl lynx crampons', 'brand': 'petzl', 'price': 99.9, 'currency': 'USD',
                'imageUrl': 'http://image.com', 'relation': {'name': 'product', 'parent': 'lynx'}
            }
        )
        print(expected_records)
        print(actual_records)
        assert actual_records == expected_records

    def test_product_record_without_origin(self):
        name = "petzl lynx crampons"
        product = {
            "url": "http://lynx.com",
            "name": name,
            "brand": "petzl",
            "currency": "USD",
            "price": 99.90,
            "store": "www.store.com",
            "imageUrl": "http://image.com"
        }
        actual_records = process_product(self.es, PRODUCT_INDEX, product)
        expected_records = ProductOriginPair({'_index': PRODUCT_INDEX, '_id': name, '_routing': name, '_op_type': 'create',
                             'brand': 'petzl', 'name': name, 'normalizedName': name, 'isCanonical': False,
                             'imageUrl': 'http://image.com', 'relation': {'name': 'origin'}},
                            {'_index': PRODUCT_INDEX, '_id': 'http://lynx.com', '_routing': name, 'url': 'http://lynx.com',
                             'store': 'www.store.com', 'name': name, 'brand': 'petzl',
                             'price': 99.9, 'currency': 'USD',
                             'imageUrl': 'http://image.com', 'relation': {'name': 'product', 'parent': name}})
        print(expected_records)
        print(actual_records)
        assert actual_records == expected_records

    def test_origin_record(self):
        origin_product = {
            "products": [
                {"name": "lynx", "brand": "petzl", "imageUrl": "http://image.com"}
            ]
        }
        actual_records = process_origin(PRODUCT_INDEX, origin_product)
        expected_records = [{'_index': 'products', '_id': 'petzl lynx', '_op_type': 'create', '_routing': 'lynx', 'isCanonical': True,
                             'name': 'lynx', 'brand': 'petzl', 'normalizedName': 'petzl lynx', "imageUrl": "http://image.com", 'relation': {'name': 'origin'}}]
        print(expected_records)
        print(actual_records)
        assert actual_records == expected_records

    def test_record_generator(self):
        event = {
            "Records": [
                {
                    "eventName": "INSERT",
                    "dynamodb": {
                        "Keys": {
                            "url": {"S": "http://test.com/product_1"}
                        },
                        "NewImage": {
                            "document": {
                                "M": {
                                    "kind": {"S": "product"},
                                    "data": {
                                        "M": {
                                            "url": {"S": "http://test.com/product_1"},
                                            "store": {"S": "test_store"},
                                            "brand": {"S": "petzl"},
                                            "name": {"S": "lynx"},
                                            "price": {"N": "10.99"},
                                            "oldPrice": {"N": "12.99"},
                                            "currency": {"S": "USD"},
                                            "imageUrl": {"S": "http://test.com/image_1.jpg"},
                                            "parseError": {"NULL": True}
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            ]
        }
        expected_records = [{'_index': 'products', '_id': 'lynx', '_routing': 'lynx', '_op_type': 'create', 'brand': 'petzl', 'origin': 'lynx', 'normalizedName': 'petzl lynx', 'relation': {'name': 'origin'}},
                            {'_index': 'products', '_id': 'http://test.com/product_1', '_routing': 'lynx', 'url': 'http://test.com/product_1', 'store': 'test_store', 'name': 'lynx', 'brand': 'petzl', 'price': Decimal('10.99'), 'currency': 'USD', 'imageUrl': 'http://test.com/image_1.jpg', 'relation': {'name': 'product', 'parent': 'lynx'}, 'oldPrice': Decimal('12.99')}]
        actual_records = (list(records_generator(self.es, PRODUCT_INDEX, event['Records'])))
        print(expected_records[1])
        print(actual_records[1])
        assert expected_records[1] == actual_records[1]

    # def test_find_origin(self):
    #     origin = find_origin(self.es, PRODUCT_INDEX, "G14", "grivel")
    #     print(origin)
