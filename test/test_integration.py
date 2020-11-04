import unittest
from lambda_function import lambda_handler
import os
from elasticsearch import Elasticsearch
from time import sleep

ELASTIC_HOST = "http://localhost:9200"
INDEX = "gear"
DOC_TYPE = "product"

os.environ["ES_HOST"] = ELASTIC_HOST
os.environ["ES_INDEX"] = INDEX
os.environ["ES_TYPE"] = DOC_TYPE

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
                                "N": 10.99
                            },
                            "oldPrice": {
                                "N": 12.99
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
                                "N": 20.99
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
                                "N": 30.99
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
                                "N": 40.99
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


es = get_es_instance()


class TestDataTransfer(unittest.TestCase):

    # TODO enable the local model storage
    def test_indexing(self):
        lambda_handler(event, None)
        product_1_expected = {'url': 'http://test.com/product_1', 'store': 'test_store', 'name': 'lynx', 'normalizedName': 'lynx', 'price': 10.99, 'oldPrice': 12.99, 'currency': 'USD', 'imageUrl': 'http://test.com/image_1.jpg'}
        product_3_expected = {'url': 'http://test.com/product_3', 'store': 'test_store', 'name': 'petzl sarken', 'normalizedName': 'sarken', 'price': 30.99, 'currency': 'USD', 'imageUrl': 'http://test.com/image_3.jpg'}
        assert(self.get_actual("http://test.com/product_1") == product_1_expected)
        assert(self.get_actual("http://test.com/product_3") == product_3_expected)

    @staticmethod
    def get_actual(doc_id):
        return es.get(index=INDEX, doc_type=DOC_TYPE, id=doc_id)["_source"]

