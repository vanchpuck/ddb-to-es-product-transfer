import logging
import os
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from classification import Classifier
import boto3

logging.getLogger().setLevel(logging.INFO)
boto3.resource('dynamodb')


MODEL_DATA_BUCKET = "org.gear-scanner.data"
CLASSIFIER = Classifier(MODEL_DATA_BUCKET)
PRODUCT_INDEX = "products"
ORIGIN_INDEX = "origins"


DESERIALIZER = boto3.dynamodb.types.TypeDeserializer()


def lambda_handler(event, context):
    logging.info(event)
    es_host = os.environ['ES_HOST']
    es_product_index = os.environ.get('ES_PRODUCT_INDEX', '')
    es_origin_index = os.environ.get('ES_ORIGIN_INDEX', '')

    es = Elasticsearch(es_host, verify_certs=False)

    # logging.info("Storing product data to {} index.".format(es_index))
    # TODO print execution details
    helpers.bulk(es, records_generator(event['Records']), raise_on_error=False)

    return 'SUCCESS'


def records_generator(records):
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
                    name = data['name']
                    brand = data['brand'].lower()
                    url = data['url']
                    logging.info('Classifying product...')
                    origin = CLASSIFIER.classify(brand, name)
                    logging.info('Product origin: ' + origin)
                    es_origin = {
                        '_index': PRODUCT_INDEX,
                        '_id': origin,
                        '_routing': origin,
                        '_op_type': 'create',
                        'brand': brand,
                        'name': origin,
                        'relation': {'name': 'brand'}}
                    logging.info('{} record marked to be sent to Elasticsearch.'.format(str(es_origin)))
                    yield es_origin
                    es_product = {
                        "_index": PRODUCT_INDEX,
                        "_id": url,
                        '_routing': origin,
                        'url': data['url'],
                        'store': data['store'],
                        'name': name,
                        'brand': brand,
                        'origin': CLASSIFIER.classify(brand, name),
                        'price': data['price'],
                        'currency': data['currency'],
                        'imageUrl': data['imageUrl'],
                        'relation': {'name': 'product', 'parent': origin}
                    }
                    if 'oldPrice' in data:
                        es_product['oldPrice'] = data['oldPrice']
                    logging.info('{} record marked to be sent to Elasticsearch.'.format(str(es_product)))
                    yield es_product
                elif kind == 'originCategory':
                    logging.info('The origin record spotted')
                    origin_product_list = data['products']
                    for origin_product in origin_product_list:
                        name = origin_product['name']
                        brand = origin_product['brand'].lower()
                        es_doc = {
                            "_index": ORIGIN_INDEX,
                            "_id": name,
                            'name': name,
                            'brand': brand,
                        }
                        logging.info('{} record marked to be sent to Elasticsearch.'.format(str(es_doc)))
                        yield es_doc
                else:
                    logging.warning('{} record has unknown product type. Skipping.'.format(doc_id))
