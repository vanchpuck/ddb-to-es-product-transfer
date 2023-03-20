import logging
import os

from util import *
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection

logging.getLogger().setLevel(logging.INFO)
boto3.resource('dynamodb')


MODEL_DATA_BUCKET = "org.gear-scanner.data"
CLASSIFIER = Classifier(MODEL_DATA_BUCKET)
PRODUCT_INDEX = "products"

ES_HOST = os.environ.get('ES_HOST', '')
ES_SERVICE = 'es'
CREDENTIALS = boto3.Session().get_credentials()
AWS_ES_AUTH = AWS4Auth(CREDENTIALS.access_key, CREDENTIALS.secret_key, os.environ.get(
    "AWS_REGION", ""), ES_SERVICE, session_token=CREDENTIALS.token)
ES = Elasticsearch(
    ES_HOST,
    http_auth=AWS_ES_AUTH,
    use_ssl=True,
    verify_certs=False,
    connection_class=RequestsHttpConnection
)


def lambda_handler(event, context):
    logging.info(event)

    # TODO print execution details
    helpers.bulk(ES, records_generator(ES, PRODUCT_INDEX, event['Records']), raise_on_error=False)

    return 'SUCCESS'
