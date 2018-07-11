#!/usr/bin/env python
import uuid
from unittest import TestCase
from test.utils import random_string, now_utc_sec, now_utc_ms, create_non_fung_table
from moto import mock_dynamodb2
from mock import Mock, MagicMock
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from limiter.managers import FungibleTokenManager, NonFungibleTokenManager, TokenReservation
from limiter.exceptions import CapacityExhaustedException

class FungibleTokenManagerTest(TestCase):
    def setUp(self):
        self.table = random_string()
        self.resource_name = random_string()
        self.limit = 10
        self.window = 100
        self.manager = FungibleTokenManager(self.table, self.resource_name, self.limit, self.window)

    def test_compute_refill_amount(self):
        current_tokens = 5
        last_refill = 1530111500000
        exec_time = last_refill + 30000

        expected = 8
        actual = self.manager._compute_refill_amount(current_tokens, last_refill, exec_time)
        self.assertEquals(expected, actual)

    def test_compute_refill_amount_negative_balance(self):
        current_tokens = -7
        last_refill = 1530111500000
        exec_time = last_refill + 30000

        expected = 3
        actual = self.manager._compute_refill_amount(current_tokens, last_refill, exec_time)
        self.assertEquals(expected, actual)

    def test_compute_refill_amount_refill_lag(self):
        current_tokens = 0
        last_refill = 1530100000000
        exec_time = last_refill + 11500000

        expected = self.limit - 1
        actual = self.manager._compute_refill_amount(current_tokens, last_refill, exec_time)
        self.assertEquals(expected, actual)

    def test_get_bucket_token(self):
        account_id = random_string()
        exec_time = now_utc_ms()

        expected = {'tokens': 5, 'last_refill': now_utc_sec()}
        response = {'Attributes': expected}
        mock_table = Mock()
        mock_table.update_item = MagicMock(return_value=response)
        self.manager._table = mock_table

        actual = self.manager._get_bucket_token(account_id, exec_time)
        self.assertEquals(expected, actual)

        actual_args = mock_table.update_item.call_args_list
        self.assertEquals(1, len(actual_args))

        expected_args = [
            ({
                'Key': {
                    'resourceName': self.resource_name,
                    'accountId': account_id
                },
                'UpdateExpression': 'add tokens :dec, set lastToken :exec_time',
                'ConditionExpression': 'tokens > :min OR lastToken < :failsafe OR attribute_not_exists(tokens)',
                'ExpressionAttributeValues': {
                    ':dec': -1,
                    ':min': 0,
                    ':failsafe': exec_time - self.manager.ms_token,
                    ':exec_time': exec_time
                },
                'ReturnValues': 'ALL_NEW'
            })
        ]

        self.assertEquals(expected_args, actual_args[0])

    def test_get_bucket_token_exhausted(self):
        account_id = random_string()
        error_response = {
            'Error': {
                'Code': 'ConditionalCheckFailedException'
            }
        }

        mock_table = Mock()
        mock_table.update_item = MagicMock(side_effect=ClientError(error_response, None))
        self.manager._table = mock_table

        self.assertRaises(CapacityExhaustedException, self.manager.get_token, account_id)

    def test_refill_bucket_tokens(self):
        account_id = random_string()
        tokens = 8
        refill_time = now_utc_ms()

        mock_table = Mock()
        mock_table.update_item = Mock()
        self.manager._table = mock_table

        self.manager._refill_bucket_tokens(account_id, tokens, refill_time)

        actual_args = mock_table.update_item.call_args_list
        self.assertEquals(1, len(actual_args))

        expected_args = [
            ({
                'Key': {
                    'resourceName': self.resource_name,
                    'accountId': account_id
                },
                'UpdateExpression': 'set tokens = :tokens, lastRefill = :refill_time',
                'ConditionExpression': 'lastRefill < :refill_time',
                'ExpressionAttributeValues': {
                    ':tokens': tokens,
                    ':refill_time': refill_time
                },
                'ReturnValues': 'NONE'
            })
        ]

        self.assertEquals(expected_args, actual_args[0])

class NonFungibleTokenManagerTest(TestCase):
    def setUp(self):
        self.table = random_string()
        self.resource_name = random_string()
        self.limit = 5
        self.manager = NonFungibleTokenManager(self.table, self.resource_name, self.limit)

    @mock_dynamodb2
    def test_get_reservation(self):
        now = now_utc_sec()
        account_id = random_string()
        coordinate = '{}:{}'.format(self.resource_name, account_id)
        mock_table = create_non_fung_table(self.table)

        self.manager._table = mock_table
        self.manager.get_reservation(account_id)

        response = mock_table.query(KeyConditionExpression=Key('resourceCoordinate').eq(coordinate))
        self.assertEquals(1, response['Count'])

        items = dict(pair for item in response['Items'] for pair in item.items())
        self.assertEquals(self.resource_name, items['resourceName'])
        self.assertEquals(account_id, items['accountId'])
        self.assertTrue(items['expirationTime'] > now)
        self.assertIn('resourceId', items)

    @mock_dynamodb2
    def test_get_reservation_exhausted(self):
        now = now_utc_sec()
        account_id = random_string()
        coordinate = '{}:{}'.format(self.resource_name, account_id)

        mock_table = create_non_fung_table(self.table)
        self.manager._table = mock_table

        # Insert enough tokens to reach the limit
        for i in range(0, self.limit):
            token = {
                'resourceCoordinate': coordinate,
                'resourceName': self.resource_name,
                'accountId': account_id,
                'resourceId': 'resource-' + str(i),
                'expirationTime': now + 10000
            }
            mock_table.put_item(Item=token)

        self.assertRaises(CapacityExhaustedException, self.manager.get_reservation, account_id)

    @mock_dynamodb2
    def test_get_token_count(self):
        now = now_utc_sec()
        account_id = random_string()
        coordinate = '{}:{}'.format(self.resource_name, account_id)
        mock_table = create_non_fung_table(self.table)
        expected_count = 1

        # Insert 2 expired and 1 valid
        expired_token_1 = {
            'resourceCoordinate': coordinate,
            'resourceName': self.resource_name,
            'accountId': account_id,
            'resourceId': random_string(),
            'expirationTime': now
        }

        expired_token_2 = {
            'resourceCoordinate': coordinate,
            'resourceName': self.resource_name,
            'accountId': account_id,
            'resourceId': random_string(),
            'expirationTime': now - 1000
        }

        valid_token = {
            'resourceCoordinate': coordinate,
            'resourceName': self.resource_name,
            'accountId': account_id,
            'resourceId': random_string(),
            'expirationTime': now + 300
        }

        mock_table.put_item(Item=expired_token_1)
        mock_table.put_item(Item=expired_token_2)
        mock_table.put_item(Item=valid_token)

        self.manager._table = mock_table
        actual_count = self.manager._get_token_count(account_id, now)

        self.assertEquals(expected_count, actual_count)

    @mock_dynamodb2
    def test_get_token_count_no_tokens(self):
        now = now_utc_sec()
        account_id = random_string()
        mock_table = create_non_fung_table(self.table)
        expected_count = 0

        self.manager._table = mock_table
        actual_count = self.manager._get_token_count(account_id, now)

        self.assertEquals(expected_count, actual_count)

class TokenReservationTest(TestCase):
    def setUp(self):
        self.table_name = random_string()
        self.res_id = str(uuid.uuid4())
        self.resource_name = random_string()
        self.account_id = random_string()
        self.coordinate = '{}:{}'.format(self.resource_name, self.account_id)

    @mock_dynamodb2
    def test_delete_reservation(self):
        mock_table = create_non_fung_table(self.table_name)

        reservation = TokenReservation(self.res_id, mock_table, self.resource_name, self.account_id, self.coordinate)
        _insert_reservation(mock_table, reservation)

        response = mock_table.query(KeyConditionExpression=Key('resourceCoordinate').eq(self.coordinate))
        self.assertEquals(1, response['Count'])

        reservation.delete()

        response = mock_table.query(KeyConditionExpression=Key('resourceCoordinate').eq(self.coordinate))
        self.assertEquals(0, response['Count'])

    def test_create_after_delete(self):
        mock_table = Mock()
        mock_table.delete_item = Mock()
        mock_table.update_item = Mock()
        reservation = TokenReservation(self.res_id, mock_table, self.resource_name, self.account_id, self.coordinate)

        reservation.delete()
        self.assertRaises(ValueError, reservation.create_token, random_string())

    def test_double_create(self):
        mock_table = Mock()
        mock_table.delete_item = Mock()
        mock_table.update_item = Mock()
        reservation = TokenReservation(self.res_id, mock_table, self.resource_name, self.account_id, self.coordinate)

        reservation.create_token(random_string())
        self.assertRaises(ValueError, reservation.create_token, random_string())

def _insert_reservation(mock_table, reservation):
    reservation_item = {
        'resourceCoordinate': reservation.coordinate,
        'resourceName': reservation.resource_name,
        'accountId': reservation.account_id,
        'resourceId': reservation.id,
        'expirationTime': now_utc_sec() + 300
    }
    mock_table.put_item(Item=reservation_item)
