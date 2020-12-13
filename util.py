import logging
import os
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from classification import Classifier
import boto3
from dataclasses import dataclass

logging.getLogger().setLevel(logging.INFO)


boto3.resource('dynamodb')
DESERIALIZER = boto3.dynamodb.types.TypeDeserializer()


@dataclass
class ProductOriginPair:
    origin: dict
    product: dict


def find_origin(es: Elasticsearch, index: str, name: str, brand: str) -> str:
    response = es.search(
        index=index,
        size=1,
        body={
            "query": {
                "bool": {
                    "must": [
                        {"match": {"origin": {"query": name}}},
                        {"term": {"brand": {"value": brand}}},
                    ]
                }
            }
        }
    )
    hits = response["hits"]
    return hits["hits"][0]["_source"]["origin"] if hits["total"]["value"] > 0 else None


def process_product(es: Elasticsearch, index: str, product: dict):
    origin_dict = None
    product_dict = None
    name = product['name']
    brand = product['brand'].lower()
    url = product['url']
    logging.info('Searching for the origin...')
    origin = find_origin(es, index, name, brand)
    logging.info('Product origin: ' + str(origin))
    if origin is None:
        logging.info('Preparing origin record...')
        origin = name
        es_origin = {
            '_index': index,
            '_id': origin,
            '_routing': origin,
            '_op_type': 'create',
            'brand': brand,
            'origin': origin,
            'normalizedName': get_normalized_name(origin, brand),
            'relation': {'name': 'origin'}}
        logging.info('Origin record: ' + str(es_origin))
        origin_dict = es_origin
    logging.info('Preparing product record...')
    es_product = {
        "_index": index,
        "_id": url,
        '_routing': origin,
        'url': product['url'],
        'store': product['store'],
        'name': name,
        'brand': brand,
        'price': product['price'],
        'currency': product['currency'],
        'imageUrl': product['imageUrl'],
        'relation': {'name': 'product', 'parent': origin}
    }
    if 'oldPrice' in product:
        es_product['oldPrice'] = product['oldPrice']
    logging.info('Product record: ' + str(es_product))
    product_dict = es_product
    return ProductOriginPair(origin_dict, product_dict)


def process_origin(index: str, origin_product: dict):
    return map(lambda origin: {"_index": index, "_id": origin['name'], '_op_type': 'create', '_routing': origin['name'],
                               'name': origin['name'], 'brand': origin['brand']},
               origin_product['products'])


def records_generator(es, index, records):
    for record in records:
        doc_id = record['dynamodb']['Keys']['url']['S']
        if record["eventName"] in ["INSERT", "MODIFY"]:
            new_image = {k: DESERIALIZER.deserialize(v) for k, v in record['dynamodb']['NewImage'].items()}
            if 'document' not in new_image:
                logging.info('{} record does not contain the document field. Skipping.'.format(doc_id))
                continue
            document = new_image['document']
            if 'data' in document and 'kind' in document:
                data = document['data']
                kind = document['kind']
                if kind == 'product':
                    logging.info('The product record spotted')
                    origin_product_pair = process_product(es, index, data)
                    yield origin_product_pair.origin
                    logging.info('{} record marked to be sent to Elasticsearch.'.format(str(origin_product_pair.origin)))
                    yield origin_product_pair.product
                    logging.info('{} record marked to be sent to Elasticsearch.'.format(str(origin_product_pair.product)))
                elif kind == 'originCategory':
                    logging.info('The origin record spotted')
                    for origin in process_origin(index, data):
                        yield origin
                        logging.info('{} record marked to be sent to Elasticsearch.'.format(str(origin)))
                else:
                    logging.warning('{} record has unknown product type. Skipping.'.format(doc_id))


def get_normalized_name(name: str, brand: str) -> str:
    return name if brand in name else brand + " " + name