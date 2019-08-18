import boto3
import requests
import urllib
import logging
import os
from requests_aws4auth import AWS4Auth

logging.getLogger().setLevel(logging.INFO)


def lambda_handler(event, context):
    region = os.environ['REGION']
    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

    es_host = os.environ['ES_HOST']
    es_index = os.environ['ES_INDEX']
    es_type = os.environ['ES_TYPE']
    es_url = es_host + '/' + es_index + '/' + es_type + '/'
    headers = {"Content-Type": "application/json"}

    total_count = 0
    es_count = 0
    for record in event['Records']:
        doc_id = record['dynamodb']['Keys']['url']['S']
        logging.info("Document id: " + doc_id)
        es_doc = extract_es_doc(record)
        if es_doc:
            logging.info("ES document: " + str(es_doc))
            logging.info("Sending document to ES...")
            response = requests.put(es_url + urllib.parse.quote(es_doc['url'], safe=''), auth=awsauth, json=es_doc, headers=headers)
            logging.info("Response code: " + str(response.status_code))
            es_count += 1
        else:
            logging.info('Skipping the {} record'.format(doc_id))
        total_count += 1
    logging.info('{} out of {} records sent to ES'.format(es_count, total_count))
    return 'SUCCESS'


def extract_es_doc(record):
    document = record['dynamodb']['NewImage']['document']['M']
    logging.info("DynamoDB document: " + str(document))
    es_doc = {
        'url': document['url']['S'],
        'store': document['store']['S'],
        'name': document['name']['S'],
        'price': document['price']['N']
    } if 'NULL' in document['parseError'] else None
    return es_doc
