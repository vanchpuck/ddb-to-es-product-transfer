import logging
import os

from util import *

logging.getLogger().setLevel(logging.INFO)
boto3.resource('dynamodb')


MODEL_DATA_BUCKET = "org.gear-scanner.data"
CLASSIFIER = Classifier(MODEL_DATA_BUCKET)
PRODUCT_INDEX = "products"

ES_HOST = os.environ.get('ES_HOST', '')
ES = Elasticsearch(ES_HOST, verify_certs=False)


def lambda_handler(event, context):
    logging.info(event)

    # TODO print execution details
    helpers.bulk(ES, records_generator(ES, PRODUCT_INDEX, event['Records']), raise_on_error=False)

    return 'SUCCESS'
