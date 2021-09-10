import json
import boto3
import datetime
import logging
import math
import os
import requests
import xmltodict

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def lambda_handler(event, context):
    LOGGER.info(event)

    return {
        'statusCode': 200,
        "headers": {
            "Content-Type": "application/xml",
        },
        'body': 'esam-response-here'
    }
