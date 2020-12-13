import logging
import os
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from classification import Classifier
import boto3
from util import *

logging.getLogger().setLevel(logging.INFO)
boto3.resource('dynamodb')


MODEL_DATA_BUCKET = "org.gear-scanner.data"
CLASSIFIER = Classifier(MODEL_DATA_BUCKET)
PRODUCT_INDEX = "products"
ORIGIN_INDEX = "origins"

ES_HOST = os.environ.get('ES_HOST', '')
ES = Elasticsearch(ES_HOST, verify_certs=False)


DESERIALIZER = boto3.dynamodb.types.TypeDeserializer()


def lambda_handler(event, context):
    logging.info(event)
    # es_host = os.environ['ES_HOST']
    es_product_index = os.environ.get('ES_PRODUCT_INDEX', '')
    es_origin_index = os.environ.get('ES_ORIGIN_INDEX', '')

    # logging.info("Storing product data to {} index.".format(es_index))
    # TODO print execution details
    helpers.bulk(ES, records_generator(ES, PRODUCT_INDEX, event['Records']), raise_on_error=False)

    return 'SUCCESS'


# def records_generator(records):
#     for record in records:
#         doc_id = record['dynamodb']['Keys']['url']['S']
#         if record["eventName"] in ["INSERT", "MODIFY"]:
#             new_image = {k: DESERIALIZER.deserialize(v) for k, v in record['dynamodb']['NewImage'].items()}
#             if 'document' not in new_image:
#                 logging.info('{} record does not contain the document field. Skipping.'.format(doc_id))
#                 continue
#             document = new_image['document']
#             if 'data' in document and 'kind' in document:
#                 data = document['data']
#                 kind = document['kind']
#                 if kind == 'product':
#                     logging.info('The product record spotted')
#                     origin_product_pair = process_product(ES, PRODUCT_INDEX, data)
#                     yield origin_product_pair.origin
#                     logging.info('{} record marked to be sent to Elasticsearch.'.format(str(origin_product_pair.origin)))
#                     yield origin_product_pair.product
#                     logging.info('{} record marked to be sent to Elasticsearch.'.format(str(origin_product_pair.product)))
#                 elif kind == 'originCategory':
#                     logging.info('The origin record spotted')
#                     for origin in process_origin(PRODUCT_INDEX, data):
#                         yield origin
#                         logging.info('{} record marked to be sent to Elasticsearch.'.format(str(es_doc)))
#                 else:
#                     logging.warning('{} record has unknown product type. Skipping.'.format(doc_id))


# def find_origin(name: str, brand: str) -> str:
#     response = ES.search(
#         index=PRODUCT_INDEX,
#         body={
#             "query": {
#                 "bool": {
#                     "must": [
#                         {"match": {"name": {"query": name}}},
#                         {"term": {"brand": {"value": brand}}},
#                         {"has_child": {"type": "product", "query": {"match_all": {}}}}
#                     ]
#                 }
#             }
#         }
#     )


def get_normalized_name(name: str, brand: str) -> str:
    return name if brand not in name else brand + " " + name
