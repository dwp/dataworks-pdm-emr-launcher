#!/usr/bin/env python3

"""pdm_emr_launcher_lambda"""
import argparse
import unittest
from unittest import mock

import boto3
from datetime import datetime
from moto import mock_dynamodb2
from pdm_emr_launcher_lambda import event_handler

TEST_DATE = "2000-10-01"
SNS_TOPIC_ARN = "test-sns-topic-arn"
TABLE_NAME = "data_pipeline_metadata"

args = argparse.Namespace()
args.sns_topic = SNS_TOPIC_ARN
args.table_name = TABLE_NAME
args.log_level = "INFO"


class TestPDMLauncher(unittest.TestCase):
    @mock_dynamodb2
    @mock.patch("pdm_emr_launcher_lambda.event_handler.logger")
    def test_query_dynamo_item_exists(self, mock_logger):
        dynamodb_resource = self.mock_get_dynamodb_table(self.get_todays_date())
        result = event_handler.query_dynamo(dynamodb_resource, self.get_todays_date())
        self.assertEqual(
            result,
            [
                {
                    "Correlation_Id": "test_correlation_id",
                    "DataProduct": "ADG-full",
                    "Date": self.get_todays_date(),
                    "S3_Prefix_Analytical_DataSet": "test_s3_prefix",
                    "Snapshot_Type": "full",
                    "Date": "2020-01-02",
                    "Status": "COMPLETED",
                }
            ],
        )

    @mock_dynamodb2
    @mock.patch("pdm_emr_launcher_lambda.event_handler.logger")
    def test_query_dynamo_item_empty_result(self, mock_logger):
        dynamodb_resource = self.mock_get_dynamodb_table(TEST_DATE)
        result = event_handler.query_dynamo(dynamodb_resource, "test")
        self.assertEqual(result, [])

    @mock.patch("pdm_emr_launcher_lambda.event_handler.send_sns_message")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.setup_logging")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.get_environment_variables")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.get_dynamo_table")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.get_sns_client")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.logger")
    @mock_dynamodb2
    def test_handler_sns_message_sent(
        self,
        mock_logger,
        get_sns_client_mock,
        get_dynamo_table_mock,
        get_environment_variables_mock,
        setup_logging_mock,
        send_sns_message_mock,
    ):
        dynamodb_resource = self.mock_get_dynamodb_table(self.get_todays_date())
        get_dynamo_table_mock.return_value = dynamodb_resource

        sns_client_mock = mock.MagicMock()
        get_sns_client_mock.return_value = sns_client_mock
        get_environment_variables_mock.return_value = args

        event_handler.handler(event={}, context=None)

        send_sns_message_mock.assert_called_once_with(
            sns_client_mock,
            {
                "correlation_id": "test_correlation_id",
                "s3_prefix": "test_s3_prefix",
                "snapshot_type": "full",
                "export_date": "2020-01-02",
            },
            args.sns_topic,
        )

    @mock.patch("pdm_emr_launcher_lambda.event_handler.send_sns_message")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.setup_logging")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.get_environment_variables")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.get_dynamo_table")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.get_sns_client")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.logger")
    @mock_dynamodb2
    def test_handler_sns_message_not_sent_when_no_items_in_dynamo(
        self,
        mock_logger,
        get_sns_client_mock,
        get_dynamo_table_mock,
        get_environment_variables_mock,
        setup_logging_mock,
        send_sns_message_mock,
    ):
        dynamodb_resource = self.mock_get_dynamodb_table(TEST_DATE)
        get_dynamo_table_mock.return_value = dynamodb_resource

        sns_client_mock = mock.MagicMock()
        get_sns_client_mock.return_value = sns_client_mock
        get_environment_variables_mock.return_value = args

        event_handler.handler(event={}, context=None)

        send_sns_message_mock.assert_not_called()

    def get_todays_date(self):
        return datetime.now().strftime("%Y-%m-%d")

    @mock.patch("pdm_emr_launcher_lambda.event_handler.setup_logging")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.get_environment_variables")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.get_sns_client")
    @mock.patch("pdm_emr_launcher_lambda.event_handler.logger")
    def test_handler_invalid_environment_variable(
        self,
        mock_logger,
        get_sns_client_mock,
        get_environment_variables_mock,
        setup_logging_mock,
    ):
        sns_client_mock = mock.MagicMock()
        get_sns_client_mock.return_value = sns_client_mock
        get_environment_variables_mock.return_value = argparse.Namespace()

        with self.assertRaises(Exception) as context:
            event_handler.handle_event()

    @mock_dynamodb2
    def mock_get_dynamodb_table(self, date):
        dynamodb_client = boto3.client("dynamodb", region_name="eu-west-2")
        dynamodb_client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "Correlation_Id", "KeyType": "HASH"},  # Partition key
                {"AttributeName": "DataProduct", "KeyType": "RANGE"},  # Sort key
            ],
            AttributeDefinitions=[
                {"AttributeName": "Correlation_Id", "AttributeType": "S"},
                {"AttributeName": "DataProduct", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )
        dynamodb = boto3.resource("dynamodb", region_name="eu-west-2")
        table = dynamodb.Table(TABLE_NAME)

        item = {
            "Correlation_Id": "test_correlation_id",
            "DataProduct": "ADG-full",
            "Date": date,
            "S3_Prefix_Analytical_DataSet": "test_s3_prefix",
            "Snapshot_Type": "full",
            "Date": "2020-01-02",
            "Status": "COMPLETED",
        }

        table.put_item(TableName=TABLE_NAME, Item=item)

        return table
