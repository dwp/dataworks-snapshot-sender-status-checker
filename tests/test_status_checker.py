#!/usr/bin/env python3

import unittest
import pytest
import argparse

from copy import deepcopy
from unittest import mock
from status_checker_lambda import status_checker

EXPORTING_STATUS = "Exporting"
EXPORTED_STATUS = "Exported"
SENT_STATUS = "Sent"
SUCCESS_STATUS = "Success"
COLLECTION_1 = "collection1"
COLLECTION_2 = "collection2"
CORRELATION_ID_1 = "correlationId1"
CORRELATION_ID_FIELD_NAME = "correlation_id"
COLLECTION_NAME_FIELD_NAME = "collection_name"
SNAPSHOT_TYPE_FIELD_NAME = "snapshot_type"
EXPORT_DATE_FIELD_NAME = "export_date"
SHUTDOWN_FLAG_FIELD_NAME = "shutdown_flag"
REPROCESS_FILES_FIELD_NAME = "reprocess_files"
CORRELATION_ID_DDB_FIELD_NAME = "CorrelationId"
COLLECTION_NAME_DDB_FIELD_NAME = "CollectionName"
COLLECTION_STATUS_DDB_FIELD_NAME = "CollectionStatus"
DDB_TABLE_NAME = "TestStatusTable"
SQS_QUEUE_URL = "http://test"
SNS_TOPIC_ARN = "test_sns_arn"
EXPORT_DATE = "2021-01-01"
SNAPSHOT_TYPE = "fulls"
MESSAGE_STATUS = "test status"

args = argparse.Namespace()
args.dynamo_db_export_status_table_name = DDB_TABLE_NAME
args.monitoring_sns_topic_arn = SNS_TOPIC_ARN
args.export_state_sqs_queue_url = SQS_QUEUE_URL
args.log_level = "INFO"


class TestReplayer(unittest.TestCase):
    @mock.patch("status_checker_lambda.status_checker.send_sns_message")
    @mock.patch(
        "status_checker_lambda.status_checker.generate_monitoring_message_payload"
    )
    @mock.patch("status_checker_lambda.status_checker.check_completion_status")
    @mock.patch(
        "status_checker_lambda.status_checker.query_dynamodb_for_all_collections"
    )
    @mock.patch("status_checker_lambda.status_checker.send_sqs_message")
    @mock.patch(
        "status_checker_lambda.status_checker.generate_export_state_message_payload"
    )
    @mock.patch("status_checker_lambda.status_checker.update_status_for_collection")
    @mock.patch("status_checker_lambda.status_checker.is_collection_received")
    @mock.patch(
        "status_checker_lambda.status_checker.update_files_received_for_collection"
    )
    @mock.patch("status_checker_lambda.status_checker.check_for_mandatory_keys")
    @mock.patch("status_checker_lambda.status_checker.setup_logging")
    @mock.patch("status_checker_lambda.status_checker.get_parameters")
    @mock.patch("status_checker_lambda.status_checker.get_client")
    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_handler_when_all_collections_have_been_received(
        self,
        mock_logger,
        get_client_mock,
        get_parameters_mock,
        setup_logging_mock,
        check_for_mandatory_keys_mock,
        update_files_received_for_collection_mock,
        is_collection_received_mock,
        update_status_for_collection_mock,
        generate_export_state_message_payload_mock,
        send_sqs_message_mock,
        query_dynamodb_for_all_collections_mock,
        check_completion_status_mock,
        generate_monitoring_message_payload_mock,
        send_sns_message_mock,
    ):
        dynamodb_client_mock = mock.MagicMock()
        sqs_client_mock = mock.MagicMock()
        sns_client_mock = mock.MagicMock()
        get_client_mock_return_values = {
            "dynamodb": dynamodb_client_mock,
            "sqs": sqs_client_mock,
            "sns": sns_client_mock,
        }
        get_client_mock.side_effect = get_client_mock_return_values.get

        get_parameters_mock.return_value = args

        single_collection_result = {
            "CollectionName": SENT_STATUS,
            "FilesReceived": 1,
            "FilesSent": 1,
        }
        update_files_received_for_collection_mock.return_value = (
            single_collection_result
        )

        is_collection_received_mock.return_value = True

        expected_payload_sqs = {
            "shutdown_flag": "true",
            "correlation_id": CORRELATION_ID_1,
            "topic_name": COLLECTION_1,
            "snapshot_type": SNAPSHOT_TYPE,
            "reprocess_files": "true",
            "export_date": EXPORT_DATE,
            "send_success_indicator": "true",
        }
        generate_export_state_message_payload_mock.return_value = expected_payload_sqs

        all_collections_result = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: COLLECTION_1,
                COLLECTION_STATUS_DDB_FIELD_NAME: EXPORTING_STATUS,
            },
            {
                COLLECTION_NAME_DDB_FIELD_NAME: COLLECTION_2,
                COLLECTION_STATUS_DDB_FIELD_NAME: EXPORTING_STATUS,
            },
        ]
        query_dynamodb_for_all_collections_mock.return_value = all_collections_result

        check_completion_status_mock.return_value = True

        expected_payload_sns = {
            "severity": "Critical",
            "notification_type": "Information",
            "slack_username": "Crown Export Poller",
            "title_text": "Fulls - test status",
        }
        generate_monitoring_message_payload_mock.return_value = expected_payload_sns

        event = {
            COLLECTION_NAME_FIELD_NAME: COLLECTION_1,
            CORRELATION_ID_FIELD_NAME: CORRELATION_ID_1,
            SNAPSHOT_TYPE_FIELD_NAME: SNAPSHOT_TYPE,
            EXPORT_DATE_FIELD_NAME: EXPORT_DATE,
        }

        status_checker.handler(event, None)

        get_client_mock.assert_any_call("dynamodb")
        get_client_mock.assert_any_call("sqs")
        get_client_mock.assert_any_call("sns")

        get_parameters_mock.assert_called_once()
        setup_logging_mock.assert_called_once()
        check_for_mandatory_keys_mock.assert_called_once()

        update_files_received_for_collection_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
            COLLECTION_1,
        )

        is_collection_received_mock.assert_called_once_with(single_collection_result)

        update_status_for_collection_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
            COLLECTION_1,
            SUCCESS_STATUS,
        )

        generate_export_state_message_payload_mock.assert_called_once_with(
            SNAPSHOT_TYPE, CORRELATION_ID_1, COLLECTION_1, EXPORT_DATE, "true", "true"
        )

        send_sqs_message_mock.assert_called_once_with(
            sqs_client_mock,
            expected_payload_sqs,
            SQS_QUEUE_URL,
        )

        query_dynamodb_for_all_collections_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
        )

        check_completion_status_mock.assert_called_once_with(
            all_collections_result,
            [SUCCESS_STATUS],
        )

        generate_monitoring_message_payload_mock.assert_called_once_with(
            SNAPSHOT_TYPE, "All collections received by NiFi"
        )

        send_sns_message_mock.assert_called_once_with(
            sns_client_mock,
            expected_payload_sns,
            SNS_TOPIC_ARN,
        )

    @mock.patch("status_checker_lambda.status_checker.send_sns_message")
    @mock.patch(
        "status_checker_lambda.status_checker.generate_monitoring_message_payload"
    )
    @mock.patch("status_checker_lambda.status_checker.check_completion_status")
    @mock.patch(
        "status_checker_lambda.status_checker.query_dynamodb_for_all_collections"
    )
    @mock.patch("status_checker_lambda.status_checker.send_sqs_message")
    @mock.patch(
        "status_checker_lambda.status_checker.generate_export_state_message_payload"
    )
    @mock.patch("status_checker_lambda.status_checker.update_status_for_collection")
    @mock.patch("status_checker_lambda.status_checker.is_collection_received")
    @mock.patch(
        "status_checker_lambda.status_checker.update_files_received_for_collection"
    )
    @mock.patch("status_checker_lambda.status_checker.check_for_mandatory_keys")
    @mock.patch("status_checker_lambda.status_checker.setup_logging")
    @mock.patch("status_checker_lambda.status_checker.get_parameters")
    @mock.patch("status_checker_lambda.status_checker.get_client")
    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_handler_when_current_collection_has_been_received_but_others_have_not(
        self,
        mock_logger,
        get_client_mock,
        get_parameters_mock,
        setup_logging_mock,
        check_for_mandatory_keys_mock,
        update_files_received_for_collection_mock,
        is_collection_received_mock,
        update_status_for_collection_mock,
        generate_export_state_message_payload_mock,
        send_sqs_message_mock,
        query_dynamodb_for_all_collections_mock,
        check_completion_status_mock,
        generate_monitoring_message_payload_mock,
        send_sns_message_mock,
    ):
        dynamodb_client_mock = mock.MagicMock()
        sqs_client_mock = mock.MagicMock()
        sns_client_mock = mock.MagicMock()
        get_client_mock_return_values = {
            "dynamodb": dynamodb_client_mock,
            "sqs": sqs_client_mock,
            "sns": sns_client_mock,
        }
        get_client_mock.side_effect = get_client_mock_return_values.get

        get_parameters_mock.return_value = args

        single_collection_result = {
            "CollectionName": SENT_STATUS,
            "FilesReceived": 1,
            "FilesSent": 1,
        }
        update_files_received_for_collection_mock.return_value = (
            single_collection_result
        )

        is_collection_received_mock.return_value = True

        expected_payload_sqs = {
            "shutdown_flag": "true",
            "correlation_id": CORRELATION_ID_1,
            "topic_name": COLLECTION_1,
            "snapshot_type": SNAPSHOT_TYPE,
            "reprocess_files": "true",
            "export_date": EXPORT_DATE,
            "send_success_indicator": "true",
        }
        generate_export_state_message_payload_mock.return_value = expected_payload_sqs

        all_collections_result = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: COLLECTION_1,
                COLLECTION_STATUS_DDB_FIELD_NAME: EXPORTING_STATUS,
            },
            {
                COLLECTION_NAME_DDB_FIELD_NAME: COLLECTION_2,
                COLLECTION_STATUS_DDB_FIELD_NAME: EXPORTING_STATUS,
            },
        ]
        query_dynamodb_for_all_collections_mock.return_value = all_collections_result

        check_completion_status_mock.return_value = False

        event = {
            COLLECTION_NAME_FIELD_NAME: COLLECTION_1,
            CORRELATION_ID_FIELD_NAME: CORRELATION_ID_1,
            SNAPSHOT_TYPE_FIELD_NAME: SNAPSHOT_TYPE,
            EXPORT_DATE_FIELD_NAME: EXPORT_DATE,
        }

        status_checker.handler(event, None)

        get_client_mock.assert_any_call("dynamodb")
        get_client_mock.assert_any_call("sqs")
        get_client_mock.assert_any_call("sns")

        get_parameters_mock.assert_called_once()
        setup_logging_mock.assert_called_once()
        check_for_mandatory_keys_mock.assert_called_once()

        update_files_received_for_collection_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
            COLLECTION_1,
        )

        is_collection_received_mock.assert_called_once_with(single_collection_result)

        update_status_for_collection_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
            COLLECTION_1,
            SUCCESS_STATUS,
        )

        generate_export_state_message_payload_mock.assert_called_once_with(
            SNAPSHOT_TYPE, CORRELATION_ID_1, COLLECTION_1, EXPORT_DATE, "true", "true"
        )

        send_sqs_message_mock.assert_called_once_with(
            sqs_client_mock,
            expected_payload_sqs,
            SQS_QUEUE_URL,
        )

        query_dynamodb_for_all_collections_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
        )

        check_completion_status_mock.assert_called_once_with(
            all_collections_result,
            [SUCCESS_STATUS],
        )

        generate_monitoring_message_payload_mock.assert_not_called()
        send_sns_message_mock.assert_not_called()

    @mock.patch("status_checker_lambda.status_checker.send_sns_message")
    @mock.patch(
        "status_checker_lambda.status_checker.generate_monitoring_message_payload"
    )
    @mock.patch("status_checker_lambda.status_checker.check_completion_status")
    @mock.patch(
        "status_checker_lambda.status_checker.query_dynamodb_for_all_collections"
    )
    @mock.patch("status_checker_lambda.status_checker.send_sqs_message")
    @mock.patch(
        "status_checker_lambda.status_checker.generate_export_state_message_payload"
    )
    @mock.patch("status_checker_lambda.status_checker.update_status_for_collection")
    @mock.patch("status_checker_lambda.status_checker.is_collection_received")
    @mock.patch(
        "status_checker_lambda.status_checker.update_files_received_for_collection"
    )
    @mock.patch("status_checker_lambda.status_checker.check_for_mandatory_keys")
    @mock.patch("status_checker_lambda.status_checker.setup_logging")
    @mock.patch("status_checker_lambda.status_checker.get_parameters")
    @mock.patch("status_checker_lambda.status_checker.get_client")
    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_handler_when_current_collection_has_been_received_but_others_have_not_with_optional_parameters(
        self,
        mock_logger,
        get_client_mock,
        get_parameters_mock,
        setup_logging_mock,
        check_for_mandatory_keys_mock,
        update_files_received_for_collection_mock,
        is_collection_received_mock,
        update_status_for_collection_mock,
        generate_export_state_message_payload_mock,
        send_sqs_message_mock,
        query_dynamodb_for_all_collections_mock,
        check_completion_status_mock,
        generate_monitoring_message_payload_mock,
        send_sns_message_mock,
    ):
        dynamodb_client_mock = mock.MagicMock()
        sqs_client_mock = mock.MagicMock()
        sns_client_mock = mock.MagicMock()
        get_client_mock_return_values = {
            "dynamodb": dynamodb_client_mock,
            "sqs": sqs_client_mock,
            "sns": sns_client_mock,
        }
        get_client_mock.side_effect = get_client_mock_return_values.get

        get_parameters_mock.return_value = args

        single_collection_result = {
            "CollectionName": SENT_STATUS,
            "FilesReceived": 1,
            "FilesSent": 1,
        }
        update_files_received_for_collection_mock.return_value = (
            single_collection_result
        )

        is_collection_received_mock.return_value = True

        expected_payload_sqs = {
            "shutdown_flag": "false",
            "correlation_id": CORRELATION_ID_1,
            "topic_name": COLLECTION_1,
            "snapshot_type": SNAPSHOT_TYPE,
            "reprocess_files": "false",
            "export_date": EXPORT_DATE,
            "send_success_indicator": "true",
        }
        generate_export_state_message_payload_mock.return_value = expected_payload_sqs

        all_collections_result = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: COLLECTION_1,
                COLLECTION_STATUS_DDB_FIELD_NAME: EXPORTING_STATUS,
            },
            {
                COLLECTION_NAME_DDB_FIELD_NAME: COLLECTION_2,
                COLLECTION_STATUS_DDB_FIELD_NAME: EXPORTING_STATUS,
            },
        ]
        query_dynamodb_for_all_collections_mock.return_value = all_collections_result

        check_completion_status_mock.return_value = False

        event = {
            COLLECTION_NAME_FIELD_NAME: COLLECTION_1,
            CORRELATION_ID_FIELD_NAME: CORRELATION_ID_1,
            SNAPSHOT_TYPE_FIELD_NAME: SNAPSHOT_TYPE,
            EXPORT_DATE_FIELD_NAME: EXPORT_DATE,
            SHUTDOWN_FLAG_FIELD_NAME: "false",
            REPROCESS_FILES_FIELD_NAME: "false",
        }

        status_checker.handler(event, None)

        get_client_mock.assert_any_call("dynamodb")
        get_client_mock.assert_any_call("sqs")
        get_client_mock.assert_any_call("sns")

        get_parameters_mock.assert_called_once()
        setup_logging_mock.assert_called_once()
        check_for_mandatory_keys_mock.assert_called_once()

        update_files_received_for_collection_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
            COLLECTION_1,
        )

        is_collection_received_mock.assert_called_once_with(single_collection_result)

        update_status_for_collection_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
            COLLECTION_1,
            SUCCESS_STATUS,
        )

        generate_export_state_message_payload_mock.assert_called_once_with(
            SNAPSHOT_TYPE, CORRELATION_ID_1, COLLECTION_1, EXPORT_DATE, "false", "false"
        )

        send_sqs_message_mock.assert_called_once_with(
            sqs_client_mock,
            expected_payload_sqs,
            SQS_QUEUE_URL,
        )

        query_dynamodb_for_all_collections_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
        )

        check_completion_status_mock.assert_called_once_with(
            all_collections_result,
            [SUCCESS_STATUS],
        )

        generate_monitoring_message_payload_mock.assert_not_called()
        send_sns_message_mock.assert_not_called()

    @mock.patch("status_checker_lambda.status_checker.send_sns_message")
    @mock.patch(
        "status_checker_lambda.status_checker.generate_monitoring_message_payload"
    )
    @mock.patch("status_checker_lambda.status_checker.check_completion_status")
    @mock.patch(
        "status_checker_lambda.status_checker.query_dynamodb_for_all_collections"
    )
    @mock.patch("status_checker_lambda.status_checker.send_sqs_message")
    @mock.patch(
        "status_checker_lambda.status_checker.generate_export_state_message_payload"
    )
    @mock.patch("status_checker_lambda.status_checker.update_status_for_collection")
    @mock.patch("status_checker_lambda.status_checker.is_collection_received")
    @mock.patch(
        "status_checker_lambda.status_checker.update_files_received_for_collection"
    )
    @mock.patch("status_checker_lambda.status_checker.check_for_mandatory_keys")
    @mock.patch("status_checker_lambda.status_checker.setup_logging")
    @mock.patch("status_checker_lambda.status_checker.get_parameters")
    @mock.patch("status_checker_lambda.status_checker.get_client")
    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_handler_when_current_collection_has_not_been_received(
        self,
        mock_logger,
        get_client_mock,
        get_parameters_mock,
        setup_logging_mock,
        check_for_mandatory_keys_mock,
        update_files_received_for_collection_mock,
        is_collection_received_mock,
        update_status_for_collection_mock,
        generate_export_state_message_payload_mock,
        send_sqs_message_mock,
        query_dynamodb_for_all_collections_mock,
        check_completion_status_mock,
        generate_monitoring_message_payload_mock,
        send_sns_message_mock,
    ):
        dynamodb_client_mock = mock.MagicMock()
        sqs_client_mock = mock.MagicMock()
        sns_client_mock = mock.MagicMock()
        get_client_mock_return_values = {
            "dynamodb": dynamodb_client_mock,
            "sqs": sqs_client_mock,
            "sns": sns_client_mock,
        }
        get_client_mock.side_effect = get_client_mock_return_values.get

        get_parameters_mock.return_value = args

        single_collection_result = {
            "CollectionName": SENT_STATUS,
            "FilesReceived": 1,
            "FilesSent": 1,
        }
        update_files_received_for_collection_mock.return_value = (
            single_collection_result
        )

        is_collection_received_mock.return_value = False

        event = {
            COLLECTION_NAME_FIELD_NAME: COLLECTION_1,
            CORRELATION_ID_FIELD_NAME: CORRELATION_ID_1,
            SNAPSHOT_TYPE_FIELD_NAME: SNAPSHOT_TYPE,
            EXPORT_DATE_FIELD_NAME: EXPORT_DATE,
        }

        status_checker.handler(event, None)

        get_client_mock.assert_any_call("dynamodb")
        get_client_mock.assert_any_call("sqs")
        get_client_mock.assert_any_call("sns")

        get_parameters_mock.assert_called_once()
        setup_logging_mock.assert_called_once()
        check_for_mandatory_keys_mock.assert_called_once()

        update_files_received_for_collection_mock.assert_called_once_with(
            dynamodb_client_mock,
            DDB_TABLE_NAME,
            CORRELATION_ID_1,
            COLLECTION_1,
        )

        is_collection_received_mock.assert_called_once_with(single_collection_result)

        update_status_for_collection_mock.assert_not_called()
        generate_export_state_message_payload_mock.assert_not_called()
        send_sqs_message_mock.assert_not_called()
        query_dynamodb_for_all_collections_mock.assert_not_called()
        check_completion_status_mock.assert_not_called()
        generate_monitoring_message_payload_mock.assert_not_called()
        send_sns_message_mock.assert_not_called()

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_sns_payload_generates_valid_payload(self, mock_logger):
        expected_payload = {
            "severity": "Critical",
            "notification_type": "Information",
            "slack_username": "Crown Export Poller",
            "title_text": "Fulls - test status",
        }
        actual_payload = status_checker.generate_monitoring_message_payload(
            SNAPSHOT_TYPE, MESSAGE_STATUS
        )
        self.assertEqual(expected_payload, actual_payload)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_sqs_payload_generates_valid_payload(self, mock_logger):
        expected_payload = {
            "shutdown_flag": "true",
            "correlation_id": CORRELATION_ID_1,
            "topic_name": COLLECTION_1,
            "snapshot_type": SNAPSHOT_TYPE,
            "reprocess_files": "false",
            "export_date": EXPORT_DATE,
            "send_success_indicator": "true",
        }
        actual_payload = status_checker.generate_export_state_message_payload(
            SNAPSHOT_TYPE, CORRELATION_ID_1, COLLECTION_1, EXPORT_DATE, "true", "false"
        )
        self.assertEqual(expected_payload, actual_payload)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_sqs_payload_generates_valid_payload(self, mock_logger):
        expected_payload = {
            "shutdown_flag": "false",
            "correlation_id": CORRELATION_ID_1,
            "topic_name": COLLECTION_1,
            "snapshot_type": SNAPSHOT_TYPE,
            "reprocess_files": "true",
            "export_date": EXPORT_DATE,
            "send_success_indicator": "true",
        }
        actual_payload = status_checker.generate_export_state_message_payload(
            SNAPSHOT_TYPE, CORRELATION_ID_1, COLLECTION_1, EXPORT_DATE, "false", "true"
        )
        self.assertEqual(expected_payload, actual_payload)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_completion_status_returns_true_with_one_status_to_check(
        self,
        mock_logger,
    ):
        response_items = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_1},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTED_STATUS},
            }
        ]
        actual = status_checker.check_completion_status(
            response_items, [EXPORTED_STATUS]
        )

        self.assertEqual(True, actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_completion_status_returns_true_with_multiple_collections_and_one_status(
        self,
        mock_logger,
    ):
        response_items = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_1},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTED_STATUS},
            },
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_2},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTED_STATUS},
            },
        ]
        actual = status_checker.check_completion_status(
            response_items, [EXPORTED_STATUS]
        )

        self.assertEqual(True, actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_completion_status_returns_true_with_multiple_statuses_to_check(
        self,
        mock_logger,
    ):
        response_items = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_1},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTED_STATUS},
            }
        ]
        actual = status_checker.check_completion_status(
            response_items, [EXPORTED_STATUS, SENT_STATUS]
        )

        self.assertEqual(True, actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_completion_status_returns_true_with_multiple_collections_and_multiple_statuses(
        self,
        mock_logger,
    ):
        response_items = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_1},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTED_STATUS},
            },
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_2},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": SENT_STATUS},
            },
        ]
        actual = status_checker.check_completion_status(
            response_items, [EXPORTED_STATUS, SENT_STATUS]
        )

        self.assertEqual(True, actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_completion_status_returns_false_with_one_status_to_check(
        self,
        mock_logger,
    ):
        response_items = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_1},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTING_STATUS},
            }
        ]
        actual = status_checker.check_completion_status(response_items, [SENT_STATUS])

        self.assertEqual(False, actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_completion_status_returns_false_with_multiple_collections_and_one_status(
        self,
        mock_logger,
    ):
        response_items = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_1},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTING_STATUS},
            },
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_2},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTING_STATUS},
            },
        ]
        actual = status_checker.check_completion_status(response_items, [SENT_STATUS])

        self.assertEqual(False, actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_completion_status_returns_false_with_multiple_statuses_to_check(
        self,
        mock_logger,
    ):
        response_items = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_1},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTING_STATUS},
            }
        ]
        actual = status_checker.check_completion_status(
            response_items, [EXPORTED_STATUS, SENT_STATUS]
        )

        self.assertEqual(False, actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_completion_status_returns_false_with_multiple_collections_and_multiple_statuses(
        self,
        mock_logger,
    ):
        response_items = [
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_1},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTING_STATUS},
            },
            {
                COLLECTION_NAME_DDB_FIELD_NAME: {"S": COLLECTION_2},
                COLLECTION_STATUS_DDB_FIELD_NAME: {"S": EXPORTING_STATUS},
            },
        ]
        actual = status_checker.check_completion_status(
            response_items, [EXPORTED_STATUS, SENT_STATUS]
        )

        self.assertEqual(False, actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_required_keys_missing_raises_error(
        self,
        mock_logger,
    ):
        event = {
            "correlation_id": "test",
            "collection_name": "test",
            "snapshot_type": "test",
        }

        with pytest.raises(
            KeyError, match=r"Required keys are missing from payload: export_date"
        ) as excinfo:
            actual = status_checker.check_for_mandatory_keys(event)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_check_required_keys_present_does_not_raise_error(
        self,
        mock_logger,
    ):
        event = {
            "correlation_id": "test",
            "collection_name": "test",
            "snapshot_type": "test",
            "export_date": "test",
        }

        actual = status_checker.check_for_mandatory_keys(event)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_is_collection_received_returns_true(
        self,
        mock_logger,
    ):
        event = {
            "CollectionStatus": {"S": SENT_STATUS},
            "FilesReceived": {"N": 1},
            "FilesSent": {"N": 1},
        }

        actual = status_checker.is_collection_received(event)

        self.assertTrue(actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_is_collection_received_returns_false_when_more_files_sent(
        self,
        mock_logger,
    ):
        event = {
            "CollectionStatus": {"S": SENT_STATUS},
            "FilesReceived": {"N": 1},
            "FilesSent": {"N": 2},
        }

        actual = status_checker.is_collection_received(event)

        self.assertFalse(actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_is_collection_received_returns_false_when_more_files_received(
        self,
        mock_logger,
    ):
        event = {
            "CollectionStatus": {"S": SENT_STATUS},
            "FilesReceived": {"N": 2},
            "FilesSent": {"N": 1},
        }

        actual = status_checker.is_collection_received(event)

        self.assertFalse(actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_is_collection_received_returns_false_when_collection_status_not_sent(
        self,
        mock_logger,
    ):
        event = {
            "CollectionStatus": {"S": EXPORTED_STATUS},
            "FilesReceived": {"N": 1},
            "FilesSent": {"N": 1},
        }

        actual = status_checker.is_collection_received(event)

        self.assertFalse(actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_update_status_for_collection_sends_right_message(
        self,
        mock_logger,
    ):
        event = {
            "CollectionStatus": {"S": EXPORTED_STATUS},
            "FilesReceived": 1,
            "FilesSent": 1,
        }

        actual = status_checker.is_collection_received(event)

        self.assertFalse(actual)

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_update_status_for_collection_sends_right_message(
        self,
        mock_logger,
    ):
        dynamodb_mock = mock.MagicMock()
        dynamodb_mock.update_item = mock.MagicMock()

        status_checker.update_status_for_collection(
            dynamodb_mock, DDB_TABLE_NAME, CORRELATION_ID_1, COLLECTION_1, SENT_STATUS
        )

        dynamodb_mock.update_item.assert_called_once_with(
            TableName=DDB_TABLE_NAME,
            Key={
                "CorrelationId": {"S": CORRELATION_ID_1},
                "CollectionName": {"S": COLLECTION_1},
            },
            UpdateExpression="SET CollectionStatus = :val",
            ExpressionAttributeValues={":val": {"S": SENT_STATUS}},
            ReturnValues="ALL_NEW",
        )

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_update_files_received_for_collection_sends_right_message(
        self,
        mock_logger,
    ):
        dynamodb_mock = mock.MagicMock()
        dynamodb_mock.update_item = mock.MagicMock()

        status_checker.update_files_received_for_collection(
            dynamodb_mock, DDB_TABLE_NAME, CORRELATION_ID_1, COLLECTION_1
        )

        dynamodb_mock.update_item.assert_called_once_with(
            TableName=DDB_TABLE_NAME,
            Key={
                "CorrelationId": {"S": CORRELATION_ID_1},
                "CollectionName": {"S": COLLECTION_1},
            },
            UpdateExpression="SET FilesReceived = FilesReceived + :val",
            ExpressionAttributeValues={":val": {"N": "1"}},
            ReturnValues="ALL_NEW",
        )

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_get_single_collection_sends_right_message(
        self,
        mock_logger,
    ):
        dynamodb_mock = mock.MagicMock()
        dynamodb_mock.get_item = mock.MagicMock()

        status_checker.get_single_collection_from_dynamodb(
            dynamodb_mock, DDB_TABLE_NAME, CORRELATION_ID_1, COLLECTION_1
        )

        dynamodb_mock.get_item.assert_called_once_with(
            TableName=DDB_TABLE_NAME,
            Key={
                "CorrelationId": {"S": CORRELATION_ID_1},
                "CollectionName": {"S": COLLECTION_1},
            },
            ConsistentRead=True,
        )

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_query_all_collections_sends_right_message(
        self,
        mock_logger,
    ):
        dynamodb_mock = mock.MagicMock()
        dynamodb_mock.query = mock.MagicMock()

        status_checker.query_dynamodb_for_all_collections(
            dynamodb_mock, DDB_TABLE_NAME, CORRELATION_ID_1
        )

        dynamodb_mock.query.assert_called_once_with(
            TableName=DDB_TABLE_NAME,
            KeyConditionExpression=f"{CORRELATION_ID_DDB_FIELD_NAME} = :c",
            ExpressionAttributeValues={":c": {"S": CORRELATION_ID_1}},
            ConsistentRead=True,
        )

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_send_sqs_message_sends_right_message(
        self,
        mock_logger,
    ):
        sqs_mock = mock.MagicMock()
        sqs_mock.send_message = mock.MagicMock()

        payload = {"test_key": "test_value"}

        status_checker.send_sqs_message(sqs_mock, payload, SQS_QUEUE_URL)

        sqs_mock.send_message.assert_called_once_with(
            QueueUrl=SQS_QUEUE_URL, MessageBody='{"test_key": "test_value"}'
        )

    @mock.patch("status_checker_lambda.status_checker.logger")
    def test_send_sns_message_sends_right_message(
        self,
        mock_logger,
    ):
        sns_mock = mock.MagicMock()
        sns_mock.publish = mock.MagicMock()

        payload = {"test_key": "test_value"}

        status_checker.send_sns_message(sns_mock, payload, SNS_TOPIC_ARN)

        sns_mock.publish.assert_called_once_with(
            TopicArn=SNS_TOPIC_ARN, Message='{"test_key": "test_value"}'
        )


if __name__ == "__main__":
    unittest.main()
