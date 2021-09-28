import json
import boto3
import datetime
import logging
import math
import os
import xmltodict


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
'''
GET
../channel/get = get all channels
../channel/channel1 = get specific channel

PUT
../channel/create/channel1
../channel/update/channel

DELETE
../channel/delete/channel1
'''


def lambda_handler(event, context):
    LOGGER.info(event)

    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/xml",
        },
        'body': 'esam-response-here'
    }