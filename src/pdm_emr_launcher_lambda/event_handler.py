#!/usr/bin/env python3

"""pdm_emr_launcher_lambda"""
import argparse
import json
import socket
import logging
import os
import sys
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime as date_time

args = None
logger = None
DATA_PRODUCT_NAME = "ADG-full"
SNS_TOPIC = "adg_completion_status_sns"


def handler(event, context):
    """Handle the event from AWS.

    Args:
        event (Object): The event details from AWS
        context (Object): The context info from AWS

    """
    global logger
    logger = setup_logging("INFO")
    logger.info(f'Cloudwatch Event": {event}')
    try:
        logger.info(os.getcwd())
        handle_event(event)
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": {err}')


def handle_event(event):
    global args

    args = get_environment_variables()

    if not args.sns_topic:
        raise Exception("Required environment variable SNS_TOPIC is unset")

    if not args.table_name:
        raise Exception("Required environment variable TABLE_NAME is unset")
    today = get_todays_date()
    dynamo_client = get_dynamo_table(args.table_name)
    dynamo_items = query_dynamo(dynamo_client, today)

    if not dynamo_items:
        logger.info(
            f"No item found in DynamoDb table {args.table_name} for date {today} and DataProduct {DATA_PRODUCT_NAME}"
        )
    else:


def send_sns_message(sns_client, payload, sns_topic_arn):
    """Publishes the message to sns.

    Arguments:
        sns_client (client): The boto3 client for SQS
        payload (dict): the payload to post to SNS
        sns_topic_arn (string): the arn for the SNS topic

    """
    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Publishing payload to SNS", "payload": {dumped_payload}, "sns_topic_arn": "{sns_topic_arn}"'
    )

    return sns_client.publish(TopicArn=sns_topic_arn, Message=dumped_payload)


def get_correlation_id(dynamodb_response):
    """Retrieves the cluster_id from the event

    Arguments:
        dynamodb_response (dict): The dict resulting from scan dynamodb by DataProduct, Date, Run_Id

    Returns:
        cluster_id: The cluster_id found in the event

    """
    correlation_id = dynamodb_response['Correlation_Id']
    logger.info(f"Correlation Id {correlation_id}")
    return correlation_id


def query_dynamo(dynamo_table, today):
    """Queries the DynamoDb table

    Arguments:
        dynamo_table (table): The boto3 table for DynamoDb
        today: Today's date

    Returns:
        list: The items matching the scan operation

    """
    response = dynamo_table.scan(FilterExpression=Attr("Date").eq(today) & Attr("DataProduct").eq(DATA_PRODUCT_NAME) & Attr("Run_Id").eq(1))
    logger.info(f"Response from dynamo {response}")
    return response["Items"][0]

def get_todays_date():
    return date_time.now().strftime("%Y-%m-%d")

def generate_lambda_launcher_payload(dynamo_item):
    payload = {
        "correlation_id": dynamo_item["Correlation_Id"],
        "s3_prefix": dynamo_item["S3_Prefix"],
    }
    logger.info(f"Lambda payload: {payload}")
    return payload


def setup_logging(logger_level):
    """Set the default logger with json output."""
    the_logger = logging.getLogger()
    for old_handler in the_logger.handlers:
        the_logger.removeHandler(old_handler)

    new_handler = logging.StreamHandler(sys.stdout)
    hostname = socket.gethostname()

    json_format = (
        f'{{ "timestamp": "%(asctime)s", "log_level": "%(levelname)s", "message": "%(message)s", '
        f'"module": "%(module)s", "process":"%(process)s", '
        f'"thread": "[%(thread)s]", "host": "{hostname}" }}'
    )

    new_handler.setFormatter(logging.Formatter(json_format))
    the_logger.addHandler(new_handler)
    new_level = logging.getLevelName(logger_level)
    the_logger.setLevel(new_level)

    if the_logger.isEnabledFor(logging.DEBUG):
        boto3.set_stream_logger()
        the_logger.debug(f'Using boto3", "version": "{boto3.__version__}')

    return the_logger


def get_environment_variables():
    """Retrieve the required environment variables.

    Returns:
        args: The parsed and validated environment variables

    """
    parser = argparse.ArgumentParser()

    _args = parser.parse_args()

    if "TABLE_NAME" in os.environ:
        _args.table_name = os.environ["TABLE_NAME"]

    if "LOG_LEVEL" in os.environ:
        _args.log_level = os.environ["LOG_LEVEL"]

    return _args


def get_dynamo_table(table_name):
    """Retrieve the boto3 DynamoDb Table resource.

    Returns:
        table: A resource representing an Amazon DynamoDB Table

    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)
    return table

def get_sns_client():
    return boto3.client("sns")
