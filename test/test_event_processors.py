#!/bin/bash/env python
from unittest import TestCase
from test.utils import random_string, now_utc_ms, now_utc_sec
from moto import mock_dynamodb2
from mock import Mock, MagicMock, patch
import boto3
from boto3.dynamodb.conditions import Key
from limiter.event_processors import ProcessorPredicate, EventProcessor, EventProcessorManager

class ProcessorPredicateTest(TestCase):
    def test_valid_str_value(self):
        target_value = random_string()
        event = {'detail': {'state': target_value}}

        predicate = ProcessorPredicate('detail.state', lambda state: state == target_value)
        self.assertTrue(predicate.test(event))

    def test_valid_numeric_value(self):
        target_value = now_utc_ms()
        event = {'detail': {'time': target_value}}

        predicate = ProcessorPredicate('detail.time', lambda state: state == target_value)
        self.assertTrue(predicate.test(event))

    def test_invalid_value(self):
        target_value = random_string()
        event = {'detail': {'state': random_string()}}

        predicate = ProcessorPredicate('detail.state', lambda state: state == target_value)
        self.assertFalse(predicate.test(event))

    def test_invalid_path(self):
        target_value = random_string()
        event = {'detail': {'state': target_value}}

        predicate = ProcessorPredicate('detail.states', lambda state: state == target_value)
        self.assertFalse(predicate.test(event))

    def test_and_all_true(self):
        state_value = random_string()
        message_value = random_string()
        zone_value = random_string()
        event = {'detail': {'state': state_value, 'message': message_value, 'zone': zone_value}}

        state_pred = ProcessorPredicate('detail.state', lambda state: state == state_value)
        message_pred = ProcessorPredicate('detail.message', lambda state: state == message_value)
        predicate = ProcessorPredicate('detail.zone', lambda state: state == zone_value)

        predicate.with_and(state_pred).with_and(message_pred)
        self.assertTrue(predicate.test(event))

    def test_and_self_false(self):
        state_value = random_string()
        message_value = random_string()
        zone_value = random_string()
        event = {'detail': {'state': state_value, 'message': message_value, 'zone': zone_value}}

        state_pred = ProcessorPredicate('detail.state', lambda state: state == state_value)
        message_pred = ProcessorPredicate('detail.message', lambda state: state == message_value)
        predicate = ProcessorPredicate('detail.zone', lambda state: state == random_string())

        predicate.with_and(state_pred).with_and(message_pred)
        self.assertFalse(predicate.test(event))

    def test_and_sibling_false(self):
        state_value = random_string()
        message_value = random_string()
        zone_value = random_string()
        event = {'detail': {'state': state_value, 'message': message_value, 'zone': zone_value}}

        state_pred = ProcessorPredicate('detail.state', lambda state: state == state_value)
        message_pred = ProcessorPredicate('detail.message', lambda state: state == message_value)
        predicate = ProcessorPredicate('detail.zone', lambda state: state == random_string())

        predicate.with_and(state_pred).with_and(message_pred)
        self.assertFalse(predicate.test(event))

    def test_or_all_true(self):
        state_value = random_string()
        message_value = random_string()
        zone_value = random_string()
        event = {'detail': {'state': state_value, 'message': message_value, 'zone': zone_value}}

        state_pred = ProcessorPredicate('detail.state', lambda state: state == state_value)
        message_pred = ProcessorPredicate('detail.message', lambda state: state == message_value)
        predicate = ProcessorPredicate('detail.zone', lambda state: state == zone_value)

        predicate.with_or(state_pred).with_or(message_pred)
        self.assertTrue(predicate.test(event))

    def test_or_self_false(self):
        state_value = random_string()
        message_value = random_string()
        zone_value = random_string()
        event = {'detail': {'state': state_value, 'message': message_value, 'zone': zone_value}}

        state_pred = ProcessorPredicate('detail.state', lambda state: state == state_value)
        message_pred = ProcessorPredicate('detail.message', lambda state: state == message_value)
        predicate = ProcessorPredicate('detail.zone', lambda state: state == random_string())

        predicate.with_or(state_pred).with_or(message_pred)
        self.assertTrue(predicate.test(event))

    def test_or_sibling_false(self):
        state_value = random_string()
        message_value = random_string()
        zone_value = random_string()
        event = {'detail': {'state': state_value, 'message': message_value, 'zone': zone_value}}

        state_pred = ProcessorPredicate('detail.state', lambda state: state == state_value)
        message_pred = ProcessorPredicate('detail.message', lambda state: state == random_string())
        predicate = ProcessorPredicate('detail.zone', lambda state: state == zone_value)

        predicate.with_or(state_pred).with_or(message_pred)
        self.assertTrue(predicate.test(event))

    def test_or_all_false(self):
        state_value = random_string()
        message_value = random_string()
        zone_value = random_string()
        event = {'detail': {'state': state_value, 'message': message_value, 'zone': zone_value}}

        state_pred = ProcessorPredicate('detail.state', lambda state: state == random_string())
        message_pred = ProcessorPredicate('detail.message', lambda state: state == random_string())
        predicate = ProcessorPredicate('detail.zone', lambda state: state == random_string())

        predicate.with_or(state_pred).with_or(message_pred)
        self.assertFalse(predicate.test(event))

class EventProcessorTest(TestCase):
    def test_with_predicate(self):
        source = random_string()
        expected_id = random_string()
        event = {'detail': {'id': expected_id}}

        mock_predicate = Mock()
        mock_predicate.test = MagicMock(return_value=True)

        processor = EventProcessor(source, 'detail.id', predicate=mock_predicate)
        actual_id = processor.test_and_get_id(event)

        self.assertEquals(expected_id, actual_id)

    def test_without_predicate(self):
        source = random_string()
        expected_id = random_string()
        event = {'detail': {'id': expected_id}}

        processor = EventProcessor(source, 'detail.id')
        actual_id = processor.test_and_get_id(event)

        self.assertEquals(expected_id, actual_id)

    def test_invalid_path(self):
        source = random_string()
        event = {'detail': {'id': random_string()}}

        mock_predicate = Mock()
        mock_predicate.test = MagicMock(return_value=True)

        processor = EventProcessor(source, 'detail.myid', predicate=mock_predicate)
        event_id = processor.test_and_get_id(event)

        self.assertIsNone(event_id)

    def test_failed_predicate(self):
        source = random_string()
        event = {'detail': {'id': random_string()}}

        mock_predicate = Mock()
        mock_predicate.test = MagicMock(return_value=False)

        processor = EventProcessor(source, 'detail.id', predicate=mock_predicate)
        event_id = processor.test_and_get_id(event)

        self.assertIsNone(event_id)

class EventProcessorManagerTest(TestCase):
    def setUp(self):
        self.table_name = random_string()
        self.index_name = random_string()
        self.coordinate = random_string()
        self.resource_name = random_string()
        self.account_id = random_string()
        self.resource_id = random_string()

    def test_no_event_source(self):
        event = {'detail': {'state': 'TERMINATED'}}

        mock_processor = Mock()
        mock_processor.source = random_string()

        manager = EventProcessorManager(table_name=self.table_name,
                                        index_name=self.index_name,
                                        processors=[mock_processor])
        self.assertRaises(ValueError, manager.process_event, event)

    def test_no_processor(self):
        event_source = random_string()
        event = {'source': event_source}

        mock_processor = Mock()
        mock_processor.source = random_string()

        manager = EventProcessorManager(table_name=self.table_name,
                                        index_name=self.index_name,
                                        processors=[mock_processor])
        self.assertRaises(ValueError, manager.process_event, event)

    def test_env_params(self):
        env_vars = {
            'NON_FUNG_TABLE': self.table_name,
            'NON_FUNG_RES_INDEX': self.index_name
        }

        with patch.dict('os.environ', env_vars):
            manager = EventProcessorManager()
            self.assertEquals(self.table_name, manager.table_name)

    @mock_dynamodb2
    def test_delete_token(self):
        event_source = random_string()
        event = {'source': event_source}

        mock_processor = Mock()
        mock_processor.source = event_source
        mock_processor.test_and_get_id = MagicMock(return_value=self.resource_id)

        mock_table = _creat_mock_table(self.table_name, self.index_name)
        self._insert_token(mock_table)
        self.assertEquals(1, self._get_resource_id_count(mock_table))

        manager = EventProcessorManager(table_name=self.table_name,
                                        index_name=self.index_name,
                                        processors=[mock_processor])
        manager._table = mock_table
        manager.process_event(event)

        self.assertEquals(0, self._get_resource_id_count(mock_table))

    @mock_dynamodb2
    def test_delete_no_token_for_id(self):
        event_source = random_string()
        event = {'source': event_source}

        mock_processor = Mock()
        mock_processor.source = event_source
        mock_processor.test_and_get_id = MagicMock(return_value=random_string())

        mock_table = _creat_mock_table(self.table_name, self.index_name)
        self._insert_token(mock_table)
        self.assertEquals(1, self._get_resource_id_count(mock_table))

        manager = EventProcessorManager(table_name=self.table_name,
                                        index_name=self.index_name,
                                        processors=[mock_processor])
        manager._table = mock_table
        manager.process_event(event)

        self.assertEquals(1, self._get_resource_id_count(mock_table))

    @mock_dynamodb2
    def test_delete_no_id_from_processor(self):
        event_source = random_string()
        event = {'source': event_source}

        mock_processor = Mock()
        mock_processor.source = event_source
        mock_processor.test_and_get_id = MagicMock(return_value=None)

        mock_table = _creat_mock_table(self.table_name, self.index_name)
        self._insert_token(mock_table)
        self.assertEquals(1, self._get_resource_id_count(mock_table))

        manager = EventProcessorManager(table_name=self.table_name,
                                        index_name=self.index_name,
                                        processors=[mock_processor])
        manager._table = mock_table
        manager.process_event(event)

        self.assertEquals(1, self._get_resource_id_count(mock_table))

    def _get_resource_id_count(self, mock_table):
        response = mock_table.query(IndexName=self.index_name,
                                    KeyConditionExpression=Key('resourceId').eq(self.resource_id))
        return response['Count']

    def _insert_token(self, mock_table):
        token_item = {
            'resourceCoordinate': self.coordinate,
            'resourceName': self.resource_name,
            'accountId': self.account_id,
            'resourceId': self.resource_id,
            'expirationTime': now_utc_sec() + 300
        }
        mock_table.put_item(Item=token_item)

def _creat_mock_table(table_name, index_name):
    mock_client = boto3.client('dynamodb', region_name='us-east-1')
    key_schema = [
        {
            'AttributeName': 'resourceCoordinate',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'resourceId',
            'KeyType': 'RANGE'
        }
    ]

    attribute_definitions = [
        {
            'AttributeName': 'resourceCoordinate',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'expirationTime',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'resourceName',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'accountId',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'resourceId',
            'AttributeType': 'S'
        }
    ]

    global_sec_indexes = [
        {
            'IndexName': index_name,
            'KeySchema': [
                {
                    'AttributeName': 'resourceId',
                    'KeyType': 'HASH'
                }
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 123,
                'WriteCapacityUnits': 123
            }
        }
    ]

    provisioned_throughput = {
        'ReadCapacityUnits': 123,
        'WriteCapacityUnits': 123
    }

    mock_client.create_table(TableName=table_name,
                             KeySchema=key_schema,
                             AttributeDefinitions=attribute_definitions,
                             GlobalSecondaryIndexes=global_sec_indexes,
                             ProvisionedThroughput=provisioned_throughput)

    return boto3.resource('dynamodb', 'us-east-1').Table(table_name)
