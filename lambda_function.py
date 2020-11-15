import logging
import os
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from classification import Classifier

logging.getLogger().setLevel(logging.INFO)


MODEL_DATA_BUCKET = "org.gear-scanner.data"
CLASSIFIER = Classifier(MODEL_DATA_BUCKET)
PRODUCT_INDEX = "products"
ORIGINAL_INDEX = "originals"


def lambda_handler(event, context):
    logging.info(event)
    es_host = os.environ['ES_HOST']
    es_product_index = os.environ.get('ES_PRODUCT_INDEX', '')
    es_original_index = os.environ.get('ES_ORIGINAL_INDEX', '')

    es = Elasticsearch(es_host, verify_certs=False)

    # logging.info("Storing product data to {} index.".format(es_index))
    helpers.bulk(es, records_generator(event['Records']))

    return 'SUCCESS'


def records_generator(records):
    for record in records:
        doc_id = record['dynamodb']['Keys']['url']['S']
        if record["eventName"] in ["INSERT", "MODIFY"]:
            new_image = record['dynamodb']['NewImage']
            if 'document' not in new_image:
                logging.info('{} record does not contain the document field. Skipping.'.format(doc_id))
                continue
            document = new_image['document']['M']
            if 'data' in document and 'kind' in document:
                data = document['data']['M']
                kind = document['kind']['S']
                if kind == 'product':
                    logging.info('The product record spotted')
                    name = data['name']['S']
                    brand = data['brand']['S'].lower()
                    url = data['url']['S']
                    es_doc = {
                        "_index": PRODUCT_INDEX,
                        "_type": "product",
                        "_id": url,
                        'url': data['url']['S'],
                        'store': data['store']['S'],
                        'name': name,
                        'brand': brand,
                        'original': CLASSIFIER.classify(brand, name),
                        'price': data['price']['N'],
                        'currency': data['currency']['S'],
                        'imageUrl': data['imageUrl']['S']
                    }
                    if 'oldPrice' in data:
                        es_doc['oldPrice'] = data['oldPrice']['N']
                    logging.info('{} record marked to be sent to Elasticsearch.'.format(str(es_doc)))
                    yield es_doc
                elif kind == 'originalCategory':
                    logging.info('The original record spotted')
                    original_product_list = data['products']['L']
                    for original_product in original_product_list:
                        name = original_product['M']['name']['S']
                        brand = original_product['M']['brand']['S'].lower()
                        es_doc = {
                            "_index": ORIGINAL_INDEX,
                            "_type": "original",
                            "_id": name,
                            'name': name,
                            'brand': brand,
                        }
                        logging.info('{} record marked to be sent to Elasticsearch.'.format(str(es_doc)))
                        yield es_doc
                else:
                    logging.warning('{} record has unknown product type. Skipping.'.format(doc_id))
