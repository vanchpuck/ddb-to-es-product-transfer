import logging
import os
from elasticsearch import helpers
from elasticsearch import Elasticsearch

logging.getLogger().setLevel(logging.INFO)


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
        if record["eventName"] == "INSERT":
            document = record['dynamodb']['NewImage']['document']['M']
            if 'NULL' not in document['parseError']:
                logging.info('{} record has parse error. Skipping.'.format(doc_id))
                continue
            logging.info('{} record are marked to be sent to Elasticsearch.'.format(doc_id))
            name = document['name']['S']
            brand = document['brand']['S']
            yield {
                "_index": index,
                "_type": type,
                "doc": {
                    'url': document['url']['S'],
                    'store': document['store']['S'],
                    'name': name,
                    'normalized_name': '{} {}'.format(brand, name) if name.find(brand) == -1 else name,
                    'price': document['price']['N'],
                    'currency': document['currency']['S']
                }
            }
