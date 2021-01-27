# dataworks-snapshot-sender-status-checker

## An AWS lambda which receives SQS messages and monitors and reports on the status of a snapshot sender run.

This repo contains Makefile to fit the standard pattern. This repo is a base to create new non-Terraform repos, adding the githooks submodule, making the repo ready for use.

After cloning this repo, please run:  
`make bootstrap`

## Function flow

This lambda will respond to SQS messages from the subscribed queues. When receiving one, it will do the following:

1. Increment the files received count for the collection in the `DYNAMO_DB_EXPORT_STATUS_TABLE_NAME` table
1. Check the `DYNAMO_DB_EXPORT_STATUS_TABLE_NAME` to see if the files received count matches the files sent count and collection status is `SENT`
1. If it is, it will post to `EXPORT_STATE_SQS_QUEUE_URL` with a message for snapshot sender to send the `_success` file to Crown
1. It will also update collection status to `RECEIVED`

If it passes the check above on file count, then the following will also be performed:

1. It will check if *all* collections for the given correlation id are in a state of `RECEIVED`
1. If they are, then it will post to `MONITORING_SNS_TOPIC_ARN` with a message for monitoring to say all collections have been received by NiFi

### Success files

If the `is_success_file` field is passed in as `true` then the flow is different to the above and instead it does the following:

1. Check the `DYNAMO_DB_EXPORT_STATUS_TABLE_NAME` to see if the collection status is `RECEIVED`
1. It will also update collection status to `SUCCESS`

If it passes the check above, then the following will also be performed:

1. It will check if *all* collections for the given correlation id are in a state of `SUCCESS`
1. If they are, then it will post to `MONITORING_SNS_TOPIC_ARN` with a message for monitoring to say all collections have been successful


## SQS message example

The following is an example SQS message to receive:

```
{
    "correlation_id": "correlation_id_1",
    "collection_name": "db.database.collection",
    "snapshot_type": "incremental",
    "export_date": "2020-01-01",
    "shutdown_flag": "true", # Defaults to true if not present
    "reprocess_files": "true", # Defaults to true if not present
    "is_success_file": "true", # Defaults to false if not present
    "filename": "folder/db.database.collection.gz.enc", # Used for logging only, defaults to NOT_SET
}
```

Required fields are:

* `correlation_id`
* `collection_name`
* `snapshot_type`
* `export_date`

Some messages sent from SQS come over in a different format. This format is also supported like below, whereby `body` must contain the same format as above (value of `body` it can be fully formed json or an escaped json string, both are supported as long as it matches the format above):

```
{
    "Records": [
        {
            "body": {
                ...
            }
        }
    ]
}
```

If the above format is used, then you can have as many records as you like as the lambda will attempt to process them all. Each body is validated against the required fields one by one.


## Environment variables

|Variable name|Example|Description|Required|
|:---|:---|:---|:---|
|AWS_PROFILE| default |The profile for making AWS calls to other services|No|
|AWS_REGION| eu-west-1 |The region the lambda is running in|No|
|ENVIRONMENT| dev |The environment the lambda is running in|No|
|APPLICATION| snapshot-sender-status-checker |The name of the application|No|
|LOG_LEVEL| INFO |The logging level of the Lambda|No|
|DYNAMO_DB_EXPORT_STATUS_TABLE_NAME|UCExportToCrownStatus|The name of the DynamoDB table used for export statuses|No|
|MONITORING_SNS_TOPIC_ARN|The arn of the sns topic to send monitoring messages to|Yes|
|EXPORT_STATE_SQS_QUEUE_URL|The sqs queue url for the export state snapshot sender messages|Yes|

## Testing

There are tox unit tests in the module. To run them, you will need the module tox installed with pip install tox, then go to the root of the module and simply run tox to run all the unit tests.

The test may also be ran via `make unittest`.

You should always ensure they work before making a pull request for your branch.

If tox has an issue with Python version you have installed, you can specify such as `tox -e py38`.