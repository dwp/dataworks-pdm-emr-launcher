#!/usr/bin/env python3

"""pdm_emr_launcher_lambda"""
import argparse
import json
import logging
import os
import socket
import sys
from datetime import datetime as date_time

import boto3
from boto3.dynamodb.conditions import Attr

CORRELATION_ID_KEY = "Correlation_Id"
DATA_PRODUCT_KEY = "DataProduct"
STATUS_KEY = "Status"
STATUS_COMPLETED = "COMPLETED"
DATE_KEY = "Date"
DATA_PRODUCT_NAME = "ADG-full"
SNS_TOPIC = "adg_completion_status_sns"

args = None
logger = None


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
    """
    Reads relevant record from dynamoDB table and sens SNS message
    """
    global args

    args = get_environment_variables()

    if not args.sns_topic:
        raise Exception("Required environment variable SNS_TOPIC is unset")

    if not args.table_name:
        raise Exception("Required environment variable TABLE_NAME is unset")

    sns_client = get_sns_client()
    today = get_export_date(event)
    correlation_id = get_correlation_id(event)
    dynamo_client = get_dynamo_table(args.table_name)
    dynamo_items = query_dynamo(dynamo_client, today, correlation_id)

    if not dynamo_items:
        logger.info(
            f"No item found in DynamoDb table {args.table_name} for date {today} and DataProduct {DATA_PRODUCT_NAME}"
        )
    else:
        logger.info(f"Sending message to SNS topic to launch PDM cluster")

        payload = generate_lambda_launcher_payload(dynamo_items[0])

        sns_response = send_sns_message(sns_client, payload, args.sns_topic)
        logger.info(f"Response from Sns: {sns_response}.")


def get_sns_client():
    return boto3.client("sns")


def send_sns_message(sns_client, payload, sns_topic_arn):
    """Publishes the message to adg_completion_status_sns SNS topic.

    Arguments:
        sns_client (client): The boto3 client for SNS
        payload (dict): the payload to post to SNS
        sns_topic_arn (string): the arn for the SNS topic

    """
    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Publishing payload to SNS", "payload": {dumped_payload}, "sns_topic_arn": "{sns_topic_arn}"'
    )

    return sns_client.publish(TopicArn=sns_topic_arn, Message=dumped_payload)


def query_dynamo(dynamo_table, today, correlation_id):
    """Queries the DynamoDb table for todays run of ADG-full application, we will just use Run_Id as 1

    Arguments:
        dynamo_table (table): The boto3 table for DynamoDb
        today: Today's date
        correlation_id: Correlation id or None if not available

    Returns:
        list: The items matching the scan operation

    """
    if correlation_id is not None:
        response = dynamo_table.scan(
            FilterExpression=Attr(DATE_KEY).eq(today)
            & Attr(CORRELATION_ID_KEY).eq(correlation_id)
            & Attr(DATA_PRODUCT_KEY).eq(DATA_PRODUCT_NAME)
            & Attr(STATUS_KEY).eq(STATUS_COMPLETED)
        )
    else:
        response = dynamo_table.scan(
            FilterExpression=Attr(DATE_KEY).eq(today)
            & Attr(DATA_PRODUCT_KEY).eq(DATA_PRODUCT_NAME)
            & Attr(STATUS_KEY).eq(STATUS_COMPLETED)
        )

    logger.info(f"Response from dynamo {response}")
    return response["Items"]


def get_export_date(event):
    if "export_date" in event:
        return event["export_date"]

    return date_time.now().strftime("%Y-%m-%d")


def get_correlation_id(event):
    if "correlation_id" in event:
        return event["correlation_id"]

    return None


def generate_lambda_launcher_payload(dynamo_item):
    payload = {
        "correlation_id": dynamo_item["Correlation_Id"],
        "s3_prefix": dynamo_item["S3_Prefix_Analytical_DataSet"],
        "snapshot_type": dynamo_item["Snapshot_Type"],
        "export_date": dynamo_item["Date"],
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

    if "SNS_TOPIC" in os.environ:
        _args.sns_topic = os.environ["SNS_TOPIC"]

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


def get_escaped_json_string(json_string):
    escaped_string = json.dumps(json_string)
    logger.info(f"Escaped json: {json_string}")
    return escaped_string
