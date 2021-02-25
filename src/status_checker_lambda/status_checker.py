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
FILE_NAME_FIELD_NAME = "file_name"
IS_SUCCESS_FILE_FIELD_NAME = "is_success_file"
SHUTDOWN_FLAG_FIELD_NAME = "shutdown_flag"
REPROCESS_FILES_FIELD_NAME = "reprocess_files"
ATTRIBUTES_FIELD_NAME = "Attributes"
ITEM_FIELD_NAME = "Item"
CORRELATION_ID_DDB_FIELD_NAME = "CorrelationId"
COLLECTION_NAME_DDB_FIELD_NAME = "CollectionName"
COLLECTION_STATUS_DDB_FIELD_NAME = "CollectionStatus"
FILES_EXPORTED_DDB_FIELD_NAME = "FilesExported"
FILES_RECEIVED_DDB_FIELD_NAME = "FilesReceived"
FILES_SENT_DDB_FIELD_NAME = "FilesSent"

EXPORTED_STATUS_VALUE = "Exported"
SENT_STATUS_VALUE = "Sent"
RECEIVED_STATUS_VALUE = "Received"
SUCCESS_STATUS_VALUE = "Success"

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


def get_escaped_json_string(json_string):
    try:
        escaped_string = json.dumps(json.dumps(json_string))
    except:  # noqa: E722
        escaped_string = json.dumps(json_string)

    return escaped_string


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


def generate_monitoring_message_payload(
    snapshot_type,
    status,
    export_date,
    correlation_id,
    file_name,
):
    """Generates a payload for a monitoring message.

    Arguments:
        snapshot_type (string): full or incremental
        status (string): the free text status for the monitoring event message
        file_name: (string) file name for logging purposes
        correlation_id: (string) the correlation id of this run
        export_date (string): the date of the export

    """
    payload = {
        "severity": "Critical",
        "notification_type": "Information",
        "slack_username": "Crown Export Poller",
        "title_text": f"{snapshot_type.title()} - {status}",
        "custom_elements": [
            {"key": "Export date", "value": export_date},
            {"key": "Correlation Id", "value": correlation_id},
        ],
    }

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Generated monitoring SNS payload", "payload": {dumped_payload}, "file_name": "{file_name}'
    )

    return payload


def generate_export_state_message_payload(
    snapshot_type,
    correlation_id,
    collection_name,
    export_date,
    shutdown_flag,
    reprocess_files,
    file_name,
):
    """Generates a payload for a monitoring message.

    Arguments:
        snapshot_type (string): full or incremental
        correlation_id (string): the correlation id for this snapshot sender run
        collection_name (string): the collection name that has been received
        export_date (string): the date of the export
        shutdown_flag (string): whether to reprocess the files on NiFi if they exist
        reprocess_files (string): whether to reprocess the files on NiFi if they exist
        file_name: file name for logging purposes

    """
    payload = {
        "shutdown_flag": shutdown_flag,
        "correlation_id": correlation_id,
        "topic_name": collection_name,
        "snapshot_type": snapshot_type,
        "reprocess_files": reprocess_files,
        "export_date": export_date,
        "send_success_indicator": "true",
    }

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Generated export SQS payload", "payload": {dumped_payload}, "file_name": "{file_name}'
    )

    return payload


def send_sns_message(
    sns_client,
    payload,
    sns_topic_arn,
    file_name,
):
    """Publishes the message to sns.

    Arguments:
        sns_client (client): The boto3 client for SQS
        payload (dict): the payload to post to SNS
        sns_topic_arn (string): the arn for the SNS topic
        file_name: file name for logging purposes

    """
    global logger

    json_message = json.dumps(payload)

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Publishing payload to SNS", "payload": {dumped_payload}, "sns_topic_arn": "{sns_topic_arn}", '
        + f'"file_name": "{file_name}'
    )

    return sns_client.publish(TopicArn=sns_topic_arn, Message=json_message)


def send_sqs_message(
    sqs_client,
    payload,
    sqs_queue_url,
    file_name,
):
    """Publishes the message to sqs.

    Arguments:
        sqs_client (client): The boto3 client for SQS
        payload (dict): the payload to post to SQS
        sqs_queue_url (string): the url of the SQS queue
        file_name: file name for logging purposes

    """
    json_message = json.dumps(payload)

    dumped_payload = get_escaped_json_string(payload)
    logger.info(
        f'Publishing payload to SQS", "payload": {dumped_payload}, "sqs_queue_url": "{sqs_queue_url}", '
        + f'"file_name": "{file_name}'
    )

    return sqs_client.send_message(QueueUrl=sqs_queue_url, MessageBody=json_message)


def check_completion_status(
    response_items,
    statuses,
    snapshot_type,
    current_collection_name,
    file_name,
):
    """Checks if all the collections are either exported or sent.

    Arguments:
        response_items: response of the  dynamo query
        statuses: an array of valid statuses equalling completion
        snapshot_type: incrementals or fulls
        current_collection_name: the current collection for logging purposes
        file_name: file name for logging purposes

    """
    logger.info(
        f'Checking completion status of all collections", "response_items": "{response_items}", '
        + f'"completed_statuses": "{statuses}", "snapshot_type": "{snapshot_type}, '
        + f'"current_collection_name": "{current_collection_name}, "file_name": "{file_name}'
    )

    is_completed = True
    for item in response_items:
        collection_status = item[COLLECTION_STATUS_DDB_FIELD_NAME]["S"]
        collection_name = item[COLLECTION_NAME_DDB_FIELD_NAME]["S"]
        logger.debug(
            "collection_status of collection %s is %s",
            collection_name,
            collection_status,
        )
        if collection_status not in statuses:
            is_completed = False
            break

    logger.info(
        f'Checked completion status of all collections", "is_completed": "{is_completed}", "file_name": "{file_name}'
    )

    return is_completed


def query_dynamodb_for_all_collections(
    dynamodb_client,
    ddb_status_table,
    correlation_id,
    file_name,
):
    """Query  DynamoDb status table for a given correlation id.

    Arguments:
        dynamodb_client (client): The boto3 client for Dynamodb
        ddb_status_table (string): The name of the Dynamodb status table
        correlation_id (string): String value of correlation-id, originates from SNS
        file_name: file name for logging purposes
    """
    logger.info(
        f'Querying for records in DynamoDb", "ddb_status_table": "{ddb_status_table}", '
        + f'"correlation_id": "{correlation_id}", "file_name": "{file_name}'
    )

    response = dynamodb_client.query(
        TableName=ddb_status_table,
        KeyConditionExpression=f"{CORRELATION_ID_DDB_FIELD_NAME} = :c",
        ExpressionAttributeValues={":c": {"S": correlation_id}},
        ConsistentRead=True,
    )
    records = response["Items"]

    logger.info(
        f'Found records in table", "ddb_status_table": "{ddb_status_table}", "record_count": "{len(records)}", '
        + f'"file_name": "{file_name}'
    )

    return records


def get_single_collection_from_dynamodb(
    dynamodb_client,
    ddb_status_table,
    correlation_id,
    collection_name,
    file_name,
):
    """Query  DynamoDb status table for a given correlation id.

    Arguments:
        dynamodb_client (client): The boto3 client for Dynamodb
        ddb_status_table (string): The name of the Dynamodb status table
        correlation_id (string): String value of CorrelationId column
        collection_name (string): String value of CollectionName column
        file_name: file name for logging purposes
    """
    logger.info(
        f'Querying for specific record in DynamoDb", "ddb_status_table": "{ddb_status_table}", "correlation_id": '
        + f'"{correlation_id}", "collection_name": "{collection_name}", "file_name": "{file_name}'
    )

    response = dynamodb_client.get_item(
        TableName=ddb_status_table,
        Key={
            CORRELATION_ID_DDB_FIELD_NAME: {"S": correlation_id},
            COLLECTION_NAME_DDB_FIELD_NAME: {"S": collection_name},
        },
        ConsistentRead=True,
    )

    logger.info(
        f'Retrieved single collection response", "ddb_status_table": "{ddb_status_table}", "correlation_id": '
        + f'"{correlation_id}", "collection_name": "{collection_name}", "response": "{response}", "file_name": "{file_name}'
    )

    return response["Item"]


def update_files_received_for_collection(
    dynamodb_client,
    ddb_status_table,
    correlation_id,
    collection_name,
    file_name,
):
    """Increment files received by one in dynamodb for the given collection name and correlation id.

    Arguments:
        dynamodb_client (client): The boto3 client for Dynamodb
        ddb_status_table (string): The name of the Dynamodb status table
        correlation_id (string): String value of CorrelationId column
        collection_name (string): String value of CollectionName column
        file_name: file name for logging purposes
    """
    logger.info(
        f'Incrementing files received count", "ddb_status_table": "{ddb_status_table}", "correlation_id": '
        + f'"{correlation_id}", "collection_name": "{collection_name}", "file_name": "{file_name}'
    )

    response = dynamodb_client.update_item(
        TableName=ddb_status_table,
        Key={
            CORRELATION_ID_DDB_FIELD_NAME: {"S": correlation_id},
            COLLECTION_NAME_DDB_FIELD_NAME: {"S": collection_name},
        },
        UpdateExpression=f"SET {FILES_RECEIVED_DDB_FIELD_NAME} = {FILES_RECEIVED_DDB_FIELD_NAME} + :val",
        ExpressionAttributeValues={":val": {"N": "1"}},
        ReturnValues="ALL_NEW",
    )

    logger.info(
        f'Incremented files received count", "ddb_status_table": "{ddb_status_table}", "correlation_id": '
        + f'"{correlation_id}", "collection_name": "{collection_name}", "response": "{response}", "file_name": "{file_name}'
    )

    return response[ATTRIBUTES_FIELD_NAME]


def update_status_for_collection(
    dynamodb_client,
    ddb_status_table,
    correlation_id,
    collection_name,
    collection_status,
    file_name,
):
    """Update the status of the collection in dynamodb for the given collection name and correlation id.

    Arguments:
        dynamodb_client (client): The boto3 client for Dynamodb
        ddb_status_table (string): The name of the Dynamodb status table
        correlation_id (string): String value of CorrelationId column
        collection_name (string): String value of CollectionName column
        collection_status (string): The status to set
        file_name: file name for logging purposes
    """
    logger.info(
        f'Updating collection status", "ddb_status_table": "{ddb_status_table}", "collection_status": '
        + f'"{collection_status}", "correlation_id": "{correlation_id}", "collection_name": "{collection_name}", "'
        + f'file_name": "{file_name}'
    )

    response = dynamodb_client.update_item(
        TableName=ddb_status_table,
        Key={
            CORRELATION_ID_DDB_FIELD_NAME: {"S": correlation_id},
            COLLECTION_NAME_DDB_FIELD_NAME: {"S": collection_name},
        },
        UpdateExpression=f"SET {COLLECTION_STATUS_DDB_FIELD_NAME} = :val",
        ExpressionAttributeValues={":val": {"S": collection_status}},
        ReturnValues="ALL_NEW",
    )

    logger.info(
        f'Updated collection status", "ddb_status_table": "{ddb_status_table}", "collection_status": '
        + f'"{collection_status}", "correlation_id": "{correlation_id}", "collection_name": "{collection_name}", '
        + f'"response": "{response}", "file_name": "{file_name}'
    )

    return response[ATTRIBUTES_FIELD_NAME]


def get_current_collection(
    dynamodb_client,
    ddb_status_table,
    correlation_id,
    collection_name,
    file_name,
):
    """Gets the item from dynamodb for the given collection name and correlation id.

    Arguments:
        dynamodb_client (client): The boto3 client for Dynamodb
        ddb_status_table (string): The name of the Dynamodb status table
        correlation_id (string): String value of CorrelationId column
        collection_name (string): String value of CollectionName column
        collection_status (string): The status to set
        file_name: file name for logging purposes
    """
    logger.info(
        f'Getting collection", "ddb_status_table": "{ddb_status_table}", "correlation_id": '
        + f'"{correlation_id}", "collection_name": "{collection_name}", "file_name": "{file_name}'
    )

    response = dynamodb_client.get_item(
        TableName=ddb_status_table,
        Key={
            CORRELATION_ID_DDB_FIELD_NAME: {"S": correlation_id},
            COLLECTION_NAME_DDB_FIELD_NAME: {"S": collection_name},
        },
    )

    logger.info(
        f'Retrieved collection", "ddb_status_table": "{ddb_status_table}", "correlation_id": '
        + f'"{correlation_id}", "collection_name": "{collection_name}", "response": "{response}'
        + f'", "file_name": "{file_name}'
    )

    return response[ITEM_FIELD_NAME]


def check_for_mandatory_keys(
    event,
):
    """Checks for mandatory keys in the event message

    Arguments:
        event (dict): The event from AWS
    """
    logger.info(
        f'Checking for mandatory keys", "required_message_keys": "{required_message_keys}'
    )

    missing_keys = []
    for required_message_key in required_message_keys:
        if (
            required_message_key not in event
            or event[required_message_key] is None
            or event[required_message_key] == ""
        ):
            missing_keys.append(required_message_key)

    if missing_keys:
        bad_keys = ", ".join(missing_keys)
        logger.error(f'Required keys missing from payload, "missing_keys": "{bad_keys}')
        return False

    logger.info(
        f'All mandatory keys present", "required_message_keys": "{required_message_keys}'
    )
    return True


def is_collection_received(
    item,
    file_name,
):
    """Checks if a collection has been fully received.
        file_name: file name for logging purposes

    Arguments:
        item (dict): The item returned from dynamo db
    """
    collection_status = item[COLLECTION_STATUS_DDB_FIELD_NAME]["S"]
    collection_files_exported_count = item[FILES_EXPORTED_DDB_FIELD_NAME]["N"]
    collection_files_received_count = item[FILES_RECEIVED_DDB_FIELD_NAME]["N"]
    collection_files_sent_count = item[FILES_SENT_DDB_FIELD_NAME]["N"]
    collection_name = item[COLLECTION_NAME_DDB_FIELD_NAME]["S"]

    logger.info(
        f'Checking if collection has been received", "collection_status": "{collection_status}", '
        + f'"collection_files_received_count": "{collection_files_received_count}", '
        + f'"collection_files_sent_count": "{collection_files_sent_count}", '
        + f'"collection_name": "{collection_name}", "file_name": "{file_name}'
    )

    is_received = (
        collection_status in [EXPORTED_STATUS_VALUE, SENT_STATUS_VALUE]
        and collection_files_received_count == collection_files_exported_count
    )

    logger.info(
        f'Checked if collection has been received", "is_received": "{is_received}", "collection_status": '
        + f'"{collection_status}", "collection_files_received_count": "{collection_files_received_count}", '
        + f'"collection_files_sent_count": "{collection_files_sent_count}", '
        + f'"collection_name": "{collection_name}", "file_name": "{file_name}'
    )

    return is_received


def is_collection_success(
    item,
    file_name,
):
    """Checks if a collection is successful.

    Arguments:
        item (dict): The item returned from dynamo db
        file_name: file name for logging purposes
    """
    collection_status = item[COLLECTION_STATUS_DDB_FIELD_NAME]["S"]
    collection_name = item[COLLECTION_NAME_DDB_FIELD_NAME]["S"]

    logger.info(
        f'Checking if collection has been successful", "file_name": "{file_name}", '
        + f'"collection_name": "{collection_name}", "collection_status": "{collection_status}'
    )

    is_success = collection_status == RECEIVED_STATUS_VALUE

    logger.info(
        f'Checked if collection has been successful", "file_name": "{file_name}", '
        + f'"is_success": "{is_success}", "collection_name": "{collection_name}", '
        + f'"collection_status": "{collection_status}"'
    )

    return is_success


def get_client(service):
    """Gets a boto3 client for the given service.

    Arguments:
        service (string): The service name to get a client for
    """
    logger.info(f'Getting boto3 client", "service": "{service}')

    return boto3.client(service)


def extract_messages(
    event,
):
    """Extracts the messages to process for the event.

    Arguments:
        event (dict): The incoming event
    """
    logger.info("Extracting body from event")

    messages_to_process = []

    if "Records" in event:
        for record in event["Records"]:
            if "body" in record:
                body = record["body"]
                logger.info(f'Extracted a message from event", "body": {body}')
                messages_to_process.append(
                    body if type(body) is dict else json.loads(body)
                )

    if len(messages_to_process) == 0:
        logger.info(
            "No messages could be extracted so attempting to process event as one message"
        )
        messages_to_process.append(event)

    logger.info(
        f'Extracted all messages from event", "message_count": "{len(messages_to_process)}'
    )
    return messages_to_process


def process_success_file_message(
    ddb_table,
    dynamodb_client,
    sns_client,
    correlation_id,
    collection_name,
    snapshot_type,
    export_date,
    sns_topic_arn,
    file_name,
):
    """Processes an individual success files message.

    Arguments:
        ddb_table (string): The ddb table name
        dynamodb_client (object): The boto3 client for dynamo db
        sns_client (object): The boto3 client for sns
        correlation_id (string): String value of CorrelationId column
        collection_name (string): String value of CollectionName column
        snapshot_type (string): Full or incremental
        export_date (string): The export date
        sns_topic_arn (string): The arn of the SNS topic to send to
        file_name (string): For logging purposes
    """
    logger.info(
        f'Processing success file message", "file_name": "{file_name}", '
        + f'"correlation_id": "{correlation_id}", "snapshot_type": "{snapshot_type}", '
        + f'"collection_name": "{collection_name}", "export_date": "{export_date}", '
        + f'"file_name": "{file_name}'
    )

    current_collection = get_current_collection(
        dynamodb_client,
        ddb_table,
        correlation_id,
        collection_name,
        file_name,
    )

    if is_collection_success(current_collection, file_name):
        update_status_for_collection(
            dynamodb_client,
            ddb_table,
            correlation_id,
            collection_name,
            SUCCESS_STATUS_VALUE,
            file_name,
        )

        all_statuses = query_dynamodb_for_all_collections(
            dynamodb_client,
            ddb_table,
            correlation_id,
            file_name,
        )

        if check_completion_status(
            all_statuses,
            [SUCCESS_STATUS_VALUE],
            snapshot_type,
            collection_name,
            file_name,
        ):
            sns_payload = generate_monitoring_message_payload(
                snapshot_type,
                "All collections successful",
                export_date,
                correlation_id,
                file_name,
            )
            send_sns_message(
                sns_client,
                sns_payload,
                sns_topic_arn,
                file_name,
            )
        else:
            logger.info(
                f'All collections have not been successful so no further processing", "file_name": "{file_name}", '
                + f'"correlation_id": "{correlation_id}", "snapshot_type": "{snapshot_type}", '
                + f'"collection_name": "{collection_name}", "file_name": "{file_name}'
            )
    else:
        logger.info(
            f'Collection has not been successful so no further processing", "file_name": "{file_name}", '
            + f'"correlation_id": "{correlation_id}", "snapshot_type": "{snapshot_type}", '
            + f'"collection_name": "{collection_name}", "file_name": "{file_name}'
        )


def process_normal_file_message(
    ddb_table,
    dynamodb_client,
    sns_client,
    sqs_client,
    correlation_id,
    collection_name,
    snapshot_type,
    export_date,
    shutdown_flag,
    reprocess_files,
    sns_topic_arn,
    sqs_queue_url,
    file_name,
):
    """Processes an individual normal files message (not a success file).

    Arguments:
        ddb_table (string): The ddb table name
        dynamodb_client (object): The boto3 client for dynamo db
        sns_client (object): The boto3 client for sns
        sqs_client (object): The boto3 client for sqs
        correlation_id (string): String value of CorrelationId column
        collection_name (string): String value of CollectionName column
        snapshot_type (string): Full or incremental
        export_date (string): The export date
        shutdown_flag (string): The shutdown flag
        reprocess_files (string): The reprocess files
        sns_topic_arn (string): The arn of the SNS topic to send to
        sqs_queue_url (string): The url of the SQS queue to send to
        file_name (string): For logging purposes
    """
    logger.info(
        f'Processing normal file message", "file_name": "{file_name}", '
        + f'"correlation_id": "{correlation_id}", "snapshot_type": "{snapshot_type}", '
        + f'"collection_name": "{collection_name}", "export_date": "{export_date}", '
        + f'"file_name": "{file_name}'
    )

    updated_collection = update_files_received_for_collection(
        dynamodb_client,
        ddb_table,
        correlation_id,
        collection_name,
        file_name,
    )

    if is_collection_received(updated_collection, file_name):
        update_status_for_collection(
            dynamodb_client,
            ddb_table,
            correlation_id,
            collection_name,
            RECEIVED_STATUS_VALUE,
            file_name,
        )

        sqs_payload = generate_export_state_message_payload(
            snapshot_type,
            correlation_id,
            collection_name,
            export_date,
            shutdown_flag,
            reprocess_files,
            file_name,
        )
        send_sqs_message(
            sqs_client,
            sqs_payload,
            sqs_queue_url,
            file_name,
        )

        all_statuses = query_dynamodb_for_all_collections(
            dynamodb_client,
            ddb_table,
            correlation_id,
            file_name,
        )
        if check_completion_status(
            all_statuses,
            [RECEIVED_STATUS_VALUE, SUCCESS_STATUS_VALUE],
            snapshot_type,
            collection_name,
            file_name,
        ):
            sns_payload = generate_monitoring_message_payload(
                snapshot_type,
                "All collections received by NiFi",
                export_date,
                correlation_id,
                file_name,
            )
            send_sns_message(
                sns_client,
                sns_payload,
                sns_topic_arn,
                file_name,
            )
        else:
            logger.info(
                f'All collections have not been fully received so no further processing", "file_name": "{file_name}", '
                + f'"correlation_id": "{correlation_id}", "export_date": "{export_date}", "snapshot_type": "{snapshot_type}", '
                + f'"collection_name": "{collection_name}", "file_name": "{file_name}'
            )
    else:
        logger.info(
            f'Collection has not been fully received so no further processing", "file_name": "{file_name}", '
            + f'"correlation_id": "{correlation_id}", "export_date": "{export_date}", "snapshot_type": "{snapshot_type}", '
            + f'"collection_name": "{collection_name}", "file_name": "{file_name}'
        )


def process_message(
    message,
    dynamodb_client,
    sqs_client,
    sns_client,
    ddb_table,
    sns_topic_arn,
    sqs_queue_url,
):
    """Processes an individual message.

    Arguments:
        message (dict): The message to process
        dynamodb_client (object): The boto3 client for dynamo db
        sqs_client (object): The boto3 client for sqs
        sns_client (object): The boto3 client for sns
        ddb_table (string): The ddb table name
        sns_topic_arn (string): The arn of the SNS topic to send to
        sqs_queue_url (string): The url of the SQS queue to send to
    """
    dumped_message = get_escaped_json_string(message)
    logger.info(f'Processing new message", "message": "{dumped_message}"')

    if not check_for_mandatory_keys(message):
        return

    collection_name = message[COLLECTION_NAME_FIELD_NAME]
    correlation_id = message[CORRELATION_ID_FIELD_NAME]
    snapshot_type = message[SNAPSHOT_TYPE_FIELD_NAME]
    export_date = message[EXPORT_DATE_FIELD_NAME]

    file_name = (
        message[FILE_NAME_FIELD_NAME]
        if FILE_NAME_FIELD_NAME in message and message[FILE_NAME_FIELD_NAME] is not None
        else "NOT_SET"
    )

    is_success_file = (
        message[IS_SUCCESS_FILE_FIELD_NAME].lower() == "true"
        if IS_SUCCESS_FILE_FIELD_NAME in message
        and message[IS_SUCCESS_FILE_FIELD_NAME] is not None
        else False
    )

    shutdown_flag = (
        message[SHUTDOWN_FLAG_FIELD_NAME]
        if SHUTDOWN_FLAG_FIELD_NAME in message
        else "true"
    )
    reprocess_files = (
        message[REPROCESS_FILES_FIELD_NAME]
        if REPROCESS_FILES_FIELD_NAME in message
        else "true"
    )

    if is_success_file:
        process_success_file_message(
            ddb_table,
            dynamodb_client,
            sns_client,
            correlation_id,
            collection_name,
            snapshot_type,
            export_date,
            sns_topic_arn,
            file_name,
        )
    else:
        process_normal_file_message(
            ddb_table,
            dynamodb_client,
            sns_client,
            sqs_client,
            correlation_id,
            collection_name,
            snapshot_type,
            export_date,
            shutdown_flag,
            reprocess_files,
            sns_topic_arn,
            sqs_queue_url,
            file_name,
        )


def handler(event, context):
    global args
    global logger

    args = get_parameters()
    logger = setup_logging(args.log_level)

    dumped_event = get_escaped_json_string(event)
    logger.info(f'Processing new event", "event": "{dumped_event}"')

    dynamodb_client = get_client("dynamodb")
    sqs_client = get_client("sqs")
    sns_client = get_client("sns")

    messages = extract_messages(event)

    for message in messages:
        process_message(
            message,
            dynamodb_client,
            sqs_client,
            sns_client,
            args.dynamo_db_export_status_table_name,
            args.monitoring_sns_topic_arn,
            args.export_state_sqs_queue_url,
        )


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
        logger.error(f'Exception occurred for invocation", "error_message": {err}')
