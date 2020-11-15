import unittest
from lambda_function import lambda_handler
import os
from elasticsearch import Elasticsearch
from time import sleep

ELASTIC_HOST = "http://localhost:9200"
PRODUCT_INDEX = "products"
ORIGINAL_INDEX = "originals"
# INDEX = "or"
PRODUCT_DOC_TYPE = "product"
ORIGINAL_DOC_TYPE = "original"

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

original_product_event = {
    "Records": [
        {
            "eventName": "INSERT",
            "dynamodb": {
                "Keys": {
                    "url": {
                        "S": "http://test.com/original_category_1"
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
                                "S": "originalCategory"
                            }
                        }
                    }
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


class TestDataTransfer(unittest.TestCase):

    es = get_es_instance()

    @classmethod
    def tearDownClass(cls):
        cls.es.indices.delete(index=PRODUCT_INDEX, ignore=[400, 404])
        cls.es.indices.delete(index=ORIGINAL_INDEX, ignore=[400, 404])

    # TODO enable the local model storage
    def test_indexing(self):
        lambda_handler(event, None)
        product_1_expected = {'url': 'http://test.com/product_1', 'store': 'test_store', 'name': 'lynx', 'original': 'lynx', 'brand': 'petzl', 'price': 10.99, 'oldPrice': 12.99, 'currency': 'USD', 'imageUrl': 'http://test.com/image_1.jpg'}
        product_3_expected = {'url': 'http://test.com/product_3', 'store': 'test_store', 'name': 'petzl sarken', 'original': 'sarken', 'brand': 'petzl', 'price': 30.99, 'currency': 'USD', 'imageUrl': 'http://test.com/image_3.jpg'}
        assert(self.get_actual("http://test.com/product_1") == product_1_expected)
        assert(self.get_actual("http://test.com/product_3") == product_3_expected)

    @classmethod
    def get_actual(cls, doc_id):
        return cls.es.get(index=PRODUCT_INDEX, doc_type=PRODUCT_DOC_TYPE, id=doc_id)["_source"]

    def test_original_indexing(self):
        lambda_handler(original_product_event, None)
        # original_1_expected = {'url': 'http://test.com/original_category_1', 'store': 'test_store', 'name': 'lynx', 'original': 'lynx', 'brand': 'petzl', 'price': 10.99, 'oldPrice': 12.99, 'currency': 'USD', 'imageUrl': 'http://test.com/image_1.jpg'}
        sleep(1)
        response = self.es.search(
            index=ORIGINAL_INDEX,
            body={
                "query": {"match_all": {}}
            }
        )
        print(response)
        # assert response["count"] == 2


