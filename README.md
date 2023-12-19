# DO NOT USE THIS REPO - MIGRATED TO GITLAB

# dataworks-pdm-emr-launcher

## About the project

This application contains Python code that is responsible for launching a PDM EMR cluster. The code is intended to be deployed 
as an AWS Lambda and triggered by a Cloudwatch event. 


A DynamoDb table will be queried for additional data about the cluster:
* Status: must be `Completed`.
* S3_Prefix_Analytical_DataSet: ADG output S3 path.
* Correlation_Id: unique id the application is running for. 


The function reads Correlation_Id and S3_Prefix_Analytical_DataSet from data_pipeline_metadata dynamoDB table for DataProduct=ADG-full and Date=todays_date 
and sends an SNS message to adg_completion_status_sns SNS topic that inturn invokes pdm-emr-launcher lambda function which will then 
launch PDM EMR cluster.

Example message:
```json
{
    "correlation_id": "example_correlation_id",
    "s3_prefix": "path/path"
}
```

## Environment variables

|Variable name|Example|Description|Required|
|:---|:---|:---|:---|
|AWS_PROFILE| default |The profile for making AWS calls to other services|No|
|AWS_REGION| eu-west-1 |The region the lambda is running in|No|
|LOG_LEVEL| INFO |The logging level of the Lambda|No|
|SNS_TOPIC_ARN|arn:aws:sns:{region}:{account}:mytopic|The arn of the sns topic to send restart messages to|Yes|
|TABLE_NAME|Retry|The name of the DynamoDb table to query for Emr data|Yes|


## Local Setup

A Makefile is provided for project setup.

Run `make setup-local` This will create a Python virtual environment and install and dependencies. 


## Testing

There are tox unit tests in the module. To run them, you will need the module tox installed with pip install tox, then go to the root of the module and simply run tox to run all the unit tests.

The test may also be ran via `make unittest`.

You should always ensure they work before making a pull request for your branch.

If tox has an issue with Python version you have installed, you can specify such as `tox -e py38`.
