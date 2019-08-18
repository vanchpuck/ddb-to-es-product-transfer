import os
import unittest
import lambda_function
from unittest.mock import patch
from types import SimpleNamespace

os.environ["REGION"] = "us-east-2"
os.environ["ES_HOST"] = "http://es.com"
os.environ["ES_INDEX"] = "test_index"
os.environ["ES_TYPE"] = "test_type"

records = {
    "Records": [
        {
            "eventID": "c4ca4238a0b923820dcc509a6f75849b",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-east-2",
            "dynamodb": {
                "Keys": {
                    "url": {
                        "S": "http://product.com"
                    }
                },
                "NewImage": {
                    "document": {
                        "M": {
                            "url": {
                                "S": "http://product.com"
                            },
                            "store": {
                                "S": "store"
                            },
                            "name": {
                                "S": "product"
                            },
                            "price": {
                                "N": "10000"
                            },
                            "parseError": {
                                "NULL": "true"
                            }
                        }
                    },
                    "Id": {
                        "S": "http://product.com"
                    }
                },
                "ApproximateCreationDateTime": 1428537600,
                "SequenceNumber": "4421584500000000017450439091",
                "SizeBytes": 26,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            },
            "eventSourceARN": "arn:aws:dynamodb:us-east-2:123456789012:table/ExampleTableWithStream/stream/2015-06-27T00:48:05.899"
        },
        {
            "eventID": "c4ca4238a0b923820dcc509a6f75849b",
            "eventName": "INSERT",
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": "us-east-2",
            "dynamodb": {
                "Keys": {
                    "url": {
                        "S": "http://uncrawled.com"
                    }
                },
                "NewImage": {
                    "document": {
                        "M": {
                            "url": {
                                "S": "http://uncrawled.com"
                            },
                            "store": {
                                "S": "store"
                            },
                            "parseError": {
                                "S": "Can't be parsed"
                            }
                        }
                    },
                    "Id": {
                        "S": "http://uncrawled.com"
                    }
                },
                "ApproximateCreationDateTime": 1428537600,
                "SequenceNumber": "4421584500000000017450439091",
                "SizeBytes": 26,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            },
            "eventSourceARN": "arn:aws:dynamodb:us-east-2:123456789012:table/ExampleTableWithStream/stream/2015-06-27T00:48:05.899"
        }
    ]
}


class TestLambda(unittest.TestCase):

    def test_crawled_doc(self):
        es_doc = lambda_function.extract_es_doc(records["Records"][0])
        self.assertEqual(es_doc, {'price': '10000', 'name': 'product', 'url': 'http://product.com', 'store': 'store'})

    def test_unparsed_doc(self):
        es_doc = lambda_function.extract_es_doc(records["Records"][1])
        self.assertEqual(es_doc, None)

    @patch('requests.put', return_value=SimpleNamespace(status_code=200))
    def test_lambda_handler(self, put):
        lambda_function.lambda_handler(records, None)
        assert put.called
