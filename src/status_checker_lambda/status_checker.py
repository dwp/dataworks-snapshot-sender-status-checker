import logging
import boto3
import argparse
import os
import sys
import socket
import json

CORRELATION_ID_FIELD_NAME = "correlation_id"
COLLECTION_NAME_FIELD_NAME = "collection_name"
SNAPSHOT_TYPE_FIELD_NAME = "snapshot_type"
EXPORT_DATE_FIELD_NAME = "export_date"
CORRELATION_ID_DDB_FIELD_NAME = "CorrelationId"
COLLECTION_NAME_DDB_FIELD_NAME = "CollectionName"
COLLECTION_STATUS_DDB_FIELD_NAME = "CollectionStatus"
FILES_RECEIVED_DDB_FIELD_NAME = "FilesReceived"
FILES_SENT_DDB_FIELD_NAME = "FilesSent"

SENT_STATUS_VALUE = "Sent"
RECEIVED_STATUS_VALUE = "Received"

log_level = os.environ["LOG_LEVEL"] if "LOG_LEVEL" in os.environ else "INFO"
required_message_keys = [
    CORRELATION_ID_FIELD_NAME,
    COLLECTION_NAME_FIELD_NAME,
    SNAPSHOT_TYPE_FIELD_NAME,
    EXPORT_DATE_FIELD_NAME,
]

args = None
logger = None


def setup_logging(logger_level):
    the_logger = logging.getLogger()
    for old_handler in the_logger.handlers:
        the_logger.removeHandler(old_handler)

    new_handler = logging.StreamHandler(sys.stdout)

    hostname = socket.gethostname()

    json_format = (
        '{ "timestamp": "%(asctime)s", "log_level": "%(levelname)s", "message": "%(message)s", '
        f'"environment": "{args.environment}", "application": "{args.application}", '
        f'"module": "%(module)s", "process": "%(process)s", '
        f'"thread": "[%(thread)s]", "hostname": "{hostname}" }} '
    )

    new_handler.setFormatter(logging.Formatter(json_format))
    the_logger.addHandler(new_handler)
    new_level = logging.getLevelName(logger_level.upper())
    the_logger.setLevel(new_level)

    if the_logger.isEnabledFor(logging.DEBUG):
        boto3.set_stream_logger()
        the_logger.debug(f'Using boto3", "version": "{boto3.__version__}')

    return the_logger


def get_parameters():
    parser = argparse.ArgumentParser(
        description="An AWS lambda which receives requests and a response payload "
        "and monitors and reports on the status of a snapshot sender run."
    )

    # Parse command line inputs and set defaults
    parser.add_argument("--aws-profile", default="default")
    parser.add_argument("--aws-region", default="eu-west-2")
    parser.add_argument("--environment", default="NOT_SET")
    parser.add_argument("--application", default="NOT_SET")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--dynamo-db-export-status-table-name", default="UCExportToCrownStatus"
    )
    parser.add_argument("--monitoring-sns-topic-arn")
    parser.add_argument("--export-state-sqs-queue-url")

    _args = parser.parse_args()

    # Override arguments with environment variables where set
    if "AWS_PROFILE" in os.environ:
        _args.aws_profile = os.environ["AWS_PROFILE"]

    if "AWS_REGION" in os.environ:
        _args.aws_region = os.environ["AWS_REGION"]

    if "ENVIRONMENT" in os.environ:
        _args.environment = os.environ["ENVIRONMENT"]

    if "APPLICATION" in os.environ:
        _args.application = os.environ["APPLICATION"]

    if "LOG_LEVEL" in os.environ:
        _args.log_level = os.environ["LOG_LEVEL"]

    if "DYNAMO_DB_EXPORT_STATUS_TABLE_NAME" in os.environ:
        _args.dynamo_db_export_status_table_name = os.environ[
            "DYNAMO_DB_EXPORT_STATUS_TABLE_NAME"
        ]

    if "MONITORING_SNS_TOPIC_ARN" in os.environ:
        _args.monitoring_sns_topic_arn = os.environ["MONITORING_SNS_TOPIC_ARN"]

    if "EXPORT_STATE_SQS_QUEUE_URL" in os.environ:
        _args.export_state_sqs_queue_url = os.environ["EXPORT_STATE_SQS_QUEUE_URL"]

    required_args = ["monitoring_sns_topic_arn", "export_state_sqs_queue_url"]
    missing_args = []
    for required_message_key in required_args:
        if required_message_key not in _args:
            missing_args.append(required_message_key)
    if missing_args:
        raise argparse.ArgumentError(
            None,
            "ArgumentError: The following required arguments are missing: {}".format(
                ", ".join(missing_args)
            ),
        )

    return _args


def generate_monitoring_message_payload(snapshot_type, status):
    """Generates a payload for a monitoring message.

    Arguments:
        snapshot_type (string): full or incremental
        status (string): the free text status for the monitoring event message

    """
    return {
        "severity": "Critical",
        "notification_type": "Information",
        "slack_username": "Crown Export Poller",
        "title_text": f"{snapshot_type.title()} - {status}",
    }


def generate_export_state_message_payload(
    snapshot_type, correlation_id, collection_name, export_date
):
    """Generates a payload for a monitoring message.

    Arguments:
        snapshot_type (string): full or incremental
        correlation_id (string): the correlation id for this snapshot sender run
        collection_name (string): the collection name that has been received
        export_date (string): the date of the export

    """
    return {
        "shutdown_flag": "true",
        "correlation_id": correlation_id,
        "topic_name": collection_name,
        "snapshot_type": snapshot_type,
        "reprocess_files": "true",
        "export_date": export_date,
        "send_success_indicator": "true",
    }


def send_sns_message(sns_client, payload, sns_topic_arn):
    """Publishes the message to sns.

    Arguments:
        sns_client (client): The boto3 client for SQS
        payload (dict): the payload to post to SNS
        sns_topic_arn (string): the arn for the SNS topic

    """
    global logger

    json_message = json.dumps(payload)
    logger.info("Publishing payload: %s to the SNS topic: %s", payload, sns_topic_arn)
    return sns_client.publish(TopicArn=sns_topic_arn, Message=json_message)


def send_sqs_message(sqs_client, payload, sqs_queue_url):
    """Publishes the message to sqs.

    Arguments:
        sqs_client (client): The boto3 client for SQS
        payload (dict): the payload to post to SQS
        sqs_queue_url (string): the url of the SQS queue

    """
    json_message = json.dumps(payload)
    logger.info("Publishing payload: %s to the SQS queue: %s", payload, sqs_queue_url)
    return sqs_client.send_message(QueueUrl=sqs_queue_url, MessageBody=json_message)


def check_completion_status(response_items, statuses):
    """Checks if all the collections are either exported or sent.

    Arguments:
        response_items: response of the  dynamo query

    """
    is_completed = True
    for item in response_items:
        collection_status = item[COLLECTION_STATUS_DDB_FIELD_NAME]
        collection_name = item[COLLECTION_NAME_DDB_FIELD_NAME]
        logger.info(
            "collection_status of collection %s is %s",
            collection_name,
            collection_status,
        )
        if collection_status not in statuses:
            is_completed = False
            break
    return is_completed


def query_dynamodb_for_all_collections(
    dynamodb_client, ddb_status_table, correlation_id
):
    """Query  DynamoDb status table for a given correlation id.

    Arguments:
        dynamodb_client (client): The boto3 client for Dynamodb
        ddb_status_table (string): The name of the Dynamodb status table
        correlation_id (string): String value of correlation-id, originates from SNS
    """
    logger.info(
        'Querying for records in DynamoDb table", ddb_status_table: %s, correlation_id: %s',
        ddb_status_table,
        correlation_id,
    )
    response = dynamodb_client.query(
        TableName=ddb_status_table,
        KeyConditionExpression=f"{CORRELATION_ID_DDB_FIELD_NAME} = :c",
        ExpressionAttributeValues={":c": {"S": correlation_id}},
        ConsistentRead=True,
    )
    records = response["Items"]
    logger.info(
        "%s records found for correlation id : %s", len(records), correlation_id
    )
    return records


def get_single_collection_from_dynamodb(
    dynamodb_client, ddb_status_table, correlation_id, collection_name
):
    """Query  DynamoDb status table for a given correlation id.

    Arguments:
        dynamodb_client (client): The boto3 client for Dynamodb
        ddb_status_table (string): The name of the Dynamodb status table
        correlation_id (string): String value of CorrelationId column
        collection_name (string): String value of CollectionName column
    """
    logger.info(
        'Querying for specific record in DynamoDb table", ddb_status_table: %s, collection_name: %s, correlation_id: %s',
        ddb_status_table,
        collection_name,
        correlation_id,
    )
    response = dynamodb_client.get_item(
        TableName=ddb_status_table,
        Key={
            CORRELATION_ID_DDB_FIELD_NAME: correlation_id,
            COLLECTION_NAME_DDB_FIELD_NAME: collection_name,
        },
        ConsistentRead=True,
    )
    return response["Item"]


def update_files_received_for_collection(
    dynamodb_client, ddb_status_table, correlation_id, collection_name
):
    """Increment files received by one in dynamodb for the given collection name and correlation id.

    Arguments:
        dynamodb_client (client): The boto3 client for Dynamodb
        ddb_status_table (string): The name of the Dynamodb status table
        correlation_id (string): String value of CorrelationId column
        collection_name (string): String value of CollectionName column
    """
    logger.info(
        'Incrementing files received", ddb_status_table: %s, collection_name: %s, correlation_id: %s',
        ddb_status_table,
        collection_name,
        correlation_id,
    )

    response = dynamodb_client.update_item(
        TableName=ddb_status_table,
        Key={
            CORRELATION_ID_DDB_FIELD_NAME: correlation_id,
            COLLECTION_NAME_DDB_FIELD_NAME: collection_name,
        },
        UpdateExpression=f"SET {FILES_RECEIVED_DDB_FIELD_NAME} = {FILES_RECEIVED_DDB_FIELD_NAME} + :val",
        ExpressionAttributeValues={":val": {"N": "1"}},
        ReturnValues="ALL_NEW",
    )
    return response


def update_status_for_collection(
    dynamodb_client,
    ddb_status_table,
    correlation_id,
    collection_name,
    collection_status,
):
    """Update the status of the collection in dynamodb for the given collection name and correlation id.

    Arguments:
        dynamodb_client (client): The boto3 client for Dynamodb
        ddb_status_table (string): The name of the Dynamodb status table
        correlation_id (string): String value of CorrelationId column
        collection_name (string): String value of CollectionName column
        collection_status (string): The status to set
    """
    logger.info(
        'Updating collection status", ddb_status_table: %s, collection_status: %s, collection_name: %s, correlation_id: %s',
        ddb_status_table,
        collection_status,
        collection_name,
        correlation_id,
    )

    response = dynamodb_client.update_item(
        TableName=ddb_status_table,
        Key={
            CORRELATION_ID_DDB_FIELD_NAME: correlation_id,
            COLLECTION_NAME_DDB_FIELD_NAME: collection_name,
        },
        UpdateExpression=f"SET {COLLECTION_STATUS_DDB_FIELD_NAME} = :val",
        ExpressionAttributeValues={":val": {"S": collection_status}},
        ReturnValues="ALL_NEW",
    )
    return response


def check_for_mandatory_keys(event):
    """Checks for mandatory keys in the event message

    Arguments:
        event (dict): The event from AWS
    """
    missing_keys = []
    for required_message_key in required_message_keys:
        if required_message_key not in event:
            missing_keys.append(required_message_key)

    if missing_keys:
        bad_keys = ", ".join(missing_keys)
        error_message = f"Required keys are missing from payload: {bad_keys}"
        raise KeyError(error_message)


def is_collection_received(item):
    """Checks if a collection has been fully received.

    Arguments:
        item (dict): The item returned from dynamo db
    """
    return (
        item[COLLECTION_NAME_DDB_FIELD_NAME] == SENT_STATUS_VALUE
        and item[FILES_RECEIVED_DDB_FIELD_NAME] == item[FILES_SENT_DDB_FIELD_NAME]
    )


def get_client(service):
    """Gets a boto3 client for the given service.

    Arguments:
        service (string): The service name to get a client for
    """
    return boto3.client(service)


def handler(event, context):
    global args
    global logger

    dynamodb_client = get_client("dynamodb")
    sns_client = get_client("sns")
    sqs_client = get_client("sqs")

    args = get_parameters()
    logger = setup_logging(args.log_level)
    logger.info(f"Event: {json.dumps(event)}")

    check_for_mandatory_keys(event)

    collection_name = event[COLLECTION_NAME_FIELD_NAME]
    correlation_id = event[CORRELATION_ID_FIELD_NAME]
    snapshot_type = event[SNAPSHOT_TYPE_FIELD_NAME]
    export_date = event[EXPORT_DATE_FIELD_NAME]

    updated_item = update_files_received_for_collection(
        dynamodb_client,
        args.dynamo_db_export_status_table_name,
        correlation_id,
        collection_name,
    )

    if is_collection_received(updated_item):
        update_status_for_collection(
            dynamodb_client,
            args.dynamo_db_export_status_table_name,
            correlation_id,
            collection_name,
            RECEIVED_STATUS_VALUE,
        )

        sqs_payload = generate_export_state_message_payload(
            snapshot_type, correlation_id, collection_name, export_date
        )
        send_sqs_message(sqs_client, sqs_payload, args.export_state_sqs_queue_url)

        all_statuses = query_dynamodb_for_all_collections(
            dynamodb_client, args.dynamo_db_export_status_table_name, correlation_id
        )
        if check_completion_status(all_statuses, [RECEIVED_STATUS_VALUE]):
            sns_payload = generate_monitoring_message_payload(
                snapshot_type, "All collections received by NiFi"
            )
            send_sns_message(sns_client, sns_payload, args.monitoring_sns_topic_arn)


if __name__ == "__main__":
    try:
        args = get_parameters()
        logger = setup_logging("INFO")

        boto3.setup_default_session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        logger.info(os.getcwd())
        json_content = json.loads(open("resources/event.json", "r").read())
        handler(json_content, None)
    except Exception as err:
        logger.error(f'Exception occurred for invocation", "error_message": "{err}')
