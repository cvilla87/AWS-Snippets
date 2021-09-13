import json
import logging
import os
import urllib3

HOOK_URL = os.environ['HOOK_URL']  # It has to be defined in AWS Lambda configuration environment variables
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
http = urllib3.PoolManager()

def lambda_handler(event, _):
    logger.info('Event: %s', event)
    notification = event['Records'][0]['Sns']
    state = notification['MessageAttributes']['state']['Value']

    if state in ['STOPPED', 'FAILED', 'TIMEOUT']:
        msg = {"text": notification['Message']}
        encoded_msg = json.dumps(msg).encode('utf-8')
        resp = http.request('POST', HOOK_URL, body=encoded_msg, headers={'Content-Type': 'application/json'})
        logger.info('Response status: %s Response data: %s', resp.status, resp.data)
        return
