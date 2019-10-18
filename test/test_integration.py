import unittest
from lambda_function import lambda_handler
import os

os.environ["ES_HOST"] = "http://localhost:9200"
os.environ["ES_INDEX"] = "gear"
os.environ["ES_TYPE"] = "product"

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
                            "name": {
                                "S": "product_1"
                            },
                            "price": {
                                "N": 10.99
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
                            "name": {
                                "S": "product_2"
                            },
                            "price": {
                                "N": 20.99
                            },
                            "currency": {
                                "S": "USD"
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
                            "name": {
                                "S": "product_3"
                            },
                            "price": {
                                "N": 30.99
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
                            "name": {
                                "S": "product_4"
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
    ]
}


class TestIntegration(unittest.TestCase):
    # For debugging purposes
    def test_indexing(self):
        lambda_handler(event, None)

