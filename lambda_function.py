import logging
import os
from elasticsearch import helpers
from elasticsearch import Elasticsearch
from classification import Classifier

logging.getLogger().setLevel(logging.INFO)


MODEL_DATA_BUCKET = "org.gear-scanner.data"
CLASSIFIER = Classifier(MODEL_DATA_BUCKET)


def lambda_handler(event, context):
    logging.info(event)
    es_host = os.environ['ES_HOST']
    es_index = os.environ['ES_INDEX']
    es_type = os.environ['ES_TYPE']

    es = Elasticsearch(es_host, verify_certs=False)

    logging.info("Storing product data to {} index.".format(es_index))
    helpers.bulk(es, records_generator(event['Records'], es_index, es_type))

    return 'SUCCESS'


def records_generator(records, index, type):
    for record in records:
        doc_id = record['dynamodb']['Keys']['url']['S']
        if record["eventName"] in ["INSERT", "MODIFY"]:
            new_image = record['dynamodb']['NewImage']
            if 'document' not in new_image:
                logging.info('{} record does not contain the document field. Skipping.'.format(doc_id))
                continue
            document = new_image['document']['M']
            if 'name' not in document or 'NULL' in document['name']:
                logging.info('{} record has no product information. Skipping.'.format(doc_id))
                continue
            name = document['name']['S']
            brand = document['brand']['S']
            url = document['url']['S']
            es_doc = {
                "_index": index,
                "_type": type,
                "_id": url,
                'url': document['url']['S'],
                'store': document['store']['S'],
                'name': name,
                # 'normalizedName': '{} {}'.format(brand, name) if name.find(brand) == -1 else name,
                'originalName': CLASSIFIER.classify(brand, name),
                'price': document['price']['N'],
                'currency': document['currency']['S'],
                'imageUrl': document['imageUrl']['S'],
            }
            if 'oldPrice' in document:
                es_doc['oldPrice'] = document['oldPrice']['N']
            logging.info('{} record marked to be sent to Elasticsearch.'.format(str(es_doc)))
            yield es_doc
