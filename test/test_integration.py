import unittest
from lambda_function import *
import os
from elasticsearch import Elasticsearch
from time import sleep
import boto3

ELASTIC_HOST = "http://localhost:9200"
PRODUCT_INDEX = "products"
ORIGIN_INDEX = "origins"
# INDEX = "or"
# PRODUCT_DOC_TYPE = "product"
# ORIGINAL_DOC_TYPE = "original"

os.environ["ES_HOST"] = ELASTIC_HOST
# os.environ["ES_PRODUCT_INDEX"] = INDEX
# os.environ["ES_TYPE"] = DOC_TYPE

event = {
    "Records": [
        {
            "eventName": "INSERT",
            "dynamodb": {
                "Keys": {
                    "url": {
                        "S": "http://test.com/product_1"
                    }
                },
                "NewImage": {
                    "document": {
                        "M": {
                            "kind": {"S": "product"},
                            "data": {
                                "M": {
                                    "url": {
                                        "S": "http://test.com/product_1"
                                    },
                                    "store": {
                                        "S": "test_store"
                                    },
                                    "brand": {
                                        "S": "petzl"
                                    },
                                    "name": {
                                        "S": "lynx"
                                    },
                                    "price": {
                                        "N": "10.99"
                                    },
                                    "oldPrice": {
                                        "N": "12.99"
                                    },
                                    "currency": {
                                        "S": "USD"
                                    },
                                    "imageUrl": {
                                        "S": "http://test.com/image_1.jpg"
                                    },
                                    "parseError": {
                                        "NULL": True
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "eventName": "INSERT",
            "dynamodb": {
                "Keys": {
                    "url": {
                        "S": "http://test.com/product_2"
                    }
                },
                "NewImage": {
                    "document": {
                        "M": {
                            "kind": {"S": "product"},
                            "data": {
                                "M": {
                                    "url": {
                                        "S": "http://test.com/product_2"
                                    },
                                    "store": {
                                        "S": "test_store"
                                    },
                                    "brand": {
                                        "S": "petzl"
                                    },
                                    "name": {
                                        "S": "sarken"
                                    },
                                    "price": {
                                        "N": "20.99"
                                    },
                                    "currency": {
                                        "S": "USD"
                                    },
                                    "imageUrl": {
                                        "S": "http://test.com/image_2.jpg"
                                    },
                                    "parseError": {
                                        "S": "Some error"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "eventName": "INSERT",
            "dynamodb": {
                "Keys": {
                    "url": {
                        "S": "http://test.com/product_3"
                    }
                },
                "NewImage": {
                    "document": {
                        "M": {
                            "kind": {"S": "product"},
                            "data": {
                                "M": {
                                    "url": {
                                        "S": "http://test.com/product_3"
                                    },
                                    "store": {
                                        "S": "test_store"
                                    },
                                    "brand": {
                                        "S": "petzl"
                                    },
                                    "name": {
                                        "S": "petzl sarken"
                                    },
                                    "price": {
                                        "N": "30.99"
                                    },
                                    "currency": {
                                        "S": "USD"
                                    },
                                    "imageUrl": {
                                        "S": "http://test.com/image_3.jpg"
                                    },
                                    "parseError": {
                                        "NULL": True
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "eventName": "REMOVE",
            "dynamodb": {
                "Keys": {
                    "url": {
                        "S": "http://test.com/product_4"
                    }
                },
                "NewImage": {
                    "document": {
                        "M": {
                            "kind": {"S": "product"},
                            "data": {
                                "M": {
                                    "url": {
                                        "S": "http://test.com/product_4"
                                    },
                                    "store": {
                                        "S": "test_store"
                                    },
                                    "brand": {
                                        "S": "petzl"
                                    },
                                    "name": {
                                        "S": "petzl lynx"
                                    },
                                    "price": {
                                        "N": "40.99"
                                    },
                                    "currency": {
                                        "S": "USD"
                                    },
                                    "parseError": {
                                        "NULL": True
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            "eventName": "INSERT",
            "dynamodb": {
                "Keys": {
                    "url": {
                        "S": "http://test.com/product_5"
                    }
                },
                "NewImage": {
                    'fetchError': {'S': 'java.util.concurrent.TimeoutException'}
                }
            }
        }
    ]
}

origin_product_event = {
    "Records": [
        {
            "eventName": "INSERT",
            "dynamodb": {
                "Keys": {
                    "url": {
                        "S": "http://test.com/origin_category_1"
                    }
                },
                "NewImage": {
                    "document": {
                        "M": {
                            "data": {
                                "M": {
                                    "nextURL": {
                                        "S": "https://grivel.com/collections/crampons?page=3"
                                    },
                                    "products": {
                                        "L": [
                                            {
                                                "M": {
                                                    "brand": {
                                                        "S": "grivel"
                                                    },
                                                    "name": {
                                                        "S": "Spider"
                                                    }
                                                }
                                            },
                                            {
                                                "M": {
                                                    "brand": {
                                                        "S": "grivel"
                                                    },
                                                    "name": {
                                                        "S": "Ran"
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            },
                            "kind": {
                                "S": "originCategory"
                            }
                        }
                    }
                }
            }
        }
    ]
}

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
            "name": {
                "type": "text",
            },
            "relation": {
                "type": "join",
                "relations": {
                    "brand": "product"
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


class TestDataTransfer(unittest.TestCase):

    es = get_es_instance()

    @classmethod
    def setUpClass(cls):
        if not cls.es.indices.exists(index=PRODUCT_INDEX):
            cls.es.indices.create(PRODUCT_INDEX, body=INDEX_SETTINGS)

    @classmethod
    def tearDownClass(cls):
        cls.es.indices.delete(index=PRODUCT_INDEX, ignore=[400, 404])
        cls.es.indices.delete(index=ORIGIN_INDEX, ignore=[400, 404])

    def test_test(self):
        records_generator(event)

    # # TODO enable the local model storage
    # def test_indexing(self):
    #     lambda_handler(event, None)
    #
    #     origin_1_expected = {'brand': 'petzl', 'name': 'lynx', 'relation': {'name': 'brand'}}
    #     origin_2_expected = {'brand': 'petzl', 'name': 'sarken', 'relation': {'name': 'brand'}}
    #     product_1_expected = {'url': 'http://test.com/product_1', 'store': 'test_store', 'name': 'lynx', 'origin': 'lynx', 'brand': 'petzl', 'price': 10.99, 'oldPrice': 12.99, 'currency': 'USD', 'imageUrl': 'http://test.com/image_1.jpg', 'relation': {'name': 'product', 'parent': 'lynx'}}
    #     product_3_expected = {'url': 'http://test.com/product_3', 'store': 'test_store', 'name': 'petzl sarken', 'origin': 'sarken', 'brand': 'petzl', 'price': 30.99, 'currency': 'USD', 'imageUrl': 'http://test.com/image_3.jpg', 'relation': {'name': 'product', 'parent': 'sarken'}}
    #
    #     assert(self.get_actual("lynx") == origin_1_expected)
    #     assert(self.get_actual("sarken") == origin_2_expected)
    #     assert(self.get_actual("http://test.com/product_1") == product_1_expected)
    #     assert(self.get_actual("http://test.com/product_3") == product_3_expected)

    @classmethod
    def get_actual(cls, doc_id):
        return cls.es.get(index=PRODUCT_INDEX, id=doc_id)["_source"]

    # def test_origin_indexing(self):
    #     lambda_handler(origin_product_event, None)
    #     # origin_1_expected = {'url': 'http://test.com/origin_category_1', 'store': 'test_store', 'name': 'lynx', 'origin': 'lynx', 'brand': 'petzl', 'price': 10.99, 'oldPrice': 12.99, 'currency': 'USD', 'imageUrl': 'http://test.com/image_1.jpg'}
    #     sleep(1)
    #     response = self.es.search(
    #         index=ORIGIN_INDEX,
    #         body={
    #             # "query": {"match_all": {}}
    #             "query": {
    #                 "match": {
    #                     "name": {
    #                         "query": "Spider"
    #                     }
    #                 }
    #             }
    #         }
    #     )
    #     print(response)
    #     # assert response["count"] == 2

    #
    # def test_test(self):
    #     print("!!")
    #     boto3.resource('dynamodb')
    #     deserializer = boto3.dynamodb.types.TypeDeserializer()
    #     # print(origin_product_event["Records"][0]["dynamodb"])
    #     python_data = {k: deserializer.deserialize(v) for k,v in origin_product_event["Records"][0]["dynamodb"]["NewImage"].items()}
    #     print(python_data)
    #
    #     serializer = boto3.dynamodb.types.TypeSerializer()
    #     low_level_copy = {k: serializer.serialize(v) for k,v in {"url": "sdfsdf"}.items()}
    #     print(low_level_copy)


