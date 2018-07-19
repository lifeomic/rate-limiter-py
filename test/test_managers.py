#!/usr/bin/env python
import uuid
import random
from unittest import TestCase
from test.utils import random_string, now_utc_sec, now_utc_ms, create_non_fung_table, create_limit_table
from moto import mock_dynamodb2
from mock import Mock, MagicMock
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from limiter.managers import FungibleTokenManager, NonFungibleTokenManager, TokenReservation, _compute_refill_amount
from limiter.exceptions import CapacityExhaustedException

class FungibleTokenManagerTest(TestCase):
    def setUp(self):
        self.token_table = random_string()
        self.limit_table = random_string()
        self.resource_name = random_string()

        self.limit = 10
        self.window = 100
        self.window_ms = self.window * 1000
        self.token_ms = float(self.limit) / self.window_ms
        self.ms_token = float(self.window_ms) / self.limit

        self.manager = FungibleTokenManager(self.token_table, self.limit_table, self.resource_name, self.limit,
                                            self.window)

    def test_compute_refill_amount(self):
        current_tokens = 5
        time_since_refill = 30000

        expected = 8
        actual = _compute_refill_amount(current_tokens, time_since_refill, self.limit, self.token_ms)

        self.assertEquals(expected, actual)

    def test_compute_refill_amount_negative_balance(self):
        current_tokens = -7
        time_since_refill = 30000

        expected = 3
        actual = _compute_refill_amount(current_tokens, time_since_refill, self.limit, self.token_ms)

        self.assertEquals(expected, actual)

    def test_compute_refill_amount_refill_lag(self):
        current_tokens = 0
        time_since_refill = 11500000

        expected = self.limit - 1
        actual = _compute_refill_amount(current_tokens, time_since_refill, self.limit, self.token_ms)
        self.assertEquals(expected, actual)

    def test_get_bucket_token(self):
        account_id = random_string()
        exec_time = now_utc_ms()

        expected = {'tokens': 5, 'last_refill': now_utc_sec()}
        response = {'Attributes': expected}
        mock_token_table = Mock()
        mock_token_table.update_item = MagicMock(return_value=response)
        self.manager._token_table = mock_token_table

        actual = self.manager._get_bucket_token(account_id, exec_time, self.ms_token)
        self.assertEquals(expected, actual)

        actual_args = mock_token_table.update_item.call_args_list
        self.assertEquals(1, len(actual_args))

        expected_args = [
            ({
                'Key': {
                    'resourceName': self.resource_name,
                    'accountId': account_id
                },
                'UpdateExpression': 'add tokens :dec set lastToken = :exec_time',
                'ConditionExpression': 'tokens > :min OR lastToken < :failsafe OR attribute_not_exists(tokens)',
                'ExpressionAttributeValues': {
                    ':dec': -1,
                    ':min': 0,
                    ':failsafe': exec_time - self.ms_token,
                    ':exec_time': exec_time
                },
                'ReturnValues': 'ALL_NEW'
            })
        ]

        self.assertEquals(expected_args, actual_args[0])

    def test_get_bucket_token_exhausted(self):
        exec_time = now_utc_ms()
        account_id = random_string()
        error_response = {
            'Error': {
                'Code': 'ConditionalCheckFailedException'
            }
        }

        mock_token_table = Mock()
        mock_token_table.update_item = MagicMock(side_effect=ClientError(error_response, None))
        self.manager._token_table = mock_token_table

        self.assertRaises(CapacityExhaustedException,
                          self.manager._get_bucket_token, account_id, exec_time, self.ms_token)

    def test_refill_bucket_tokens(self):
        account_id = random_string()
        tokens = 8
        refill_time = now_utc_ms()

        mock_token_table = Mock()
        mock_token_table.update_item = Mock()
        self.manager._token_table = mock_token_table

        self.manager._refill_bucket_tokens(account_id, tokens, refill_time)

        actual_args = mock_token_table.update_item.call_args_list
        self.assertEquals(1, len(actual_args))

        expected_args = [
            ({
                'Key': {
                    'resourceName': self.resource_name,
                    'accountId': account_id
                },
                'UpdateExpression': 'set tokens = :tokens, lastRefill = :refill_time',
                'ConditionExpression': 'lastRefill < :refill_time OR attribute_not_exists(lastRefill)',
                'ExpressionAttributeValues': {
                    ':tokens': tokens,
                    ':refill_time': refill_time
                },
                'ReturnValues': 'NONE'
            })
        ]

        self.assertEquals(expected_args, actual_args[0])

    @mock_dynamodb2
    def test_account_resource_limit(self):
        limit = random.randint(0, 100)
        window = random.randint(0, 100)
        account_id = random_string()

        mock_limit_table = create_limit_table(self.limit_table)
        _insert_limit(mock_limit_table, self.resource_name, account_id, limit, window)

        self.manager._limit_table = mock_limit_table
        result = self.manager._get_account_resource_limit(account_id)

        self.assertEquals(limit, result['limit'])
        self.assertEquals(window, result['windowSec'])

    @mock_dynamodb2
    def test_account_resource_limit_defaults(self):
        account_id = random_string()

        mock_limit_table = create_limit_table(self.limit_table)
        self.manager._limit_table = mock_limit_table

        result = self.manager._get_account_resource_limit(account_id)
        self.assertEquals(self.limit, result['limit'])
        self.assertEquals(self.window, result['windowSec'])

    @mock_dynamodb2
    def test_account_resource_limit_blacklist(self):
        limit = 0
        window = random.randint(0, 100)
        account_id = random_string()

        mock_limit_table = create_limit_table(self.limit_table)
        _insert_limit(mock_limit_table, self.resource_name, account_id, limit, window)
        self.manager._limit_table = mock_limit_table

        self.assertRaises(CapacityExhaustedException, self.manager._get_account_resource_limit, account_id)

class NonFungibleTokenManagerTest(TestCase):
    def setUp(self):
        self.token_table = random_string()
        self.limit_table = random_string()
        self.resource_name = random_string()

        self.limit = 5
        self.manager = NonFungibleTokenManager(self.token_table, self.limit_table, self.resource_name, self.limit)

    @mock_dynamodb2
    def test_get_reservation(self):
        now = now_utc_sec()
        account_id = random_string()
        coordinate = '{}:{}'.format(self.resource_name, account_id)

        mock_token_table = create_non_fung_table(self.token_table)
        mock_limit_table = create_limit_table(self.limit_table)
        _insert_limit(mock_limit_table, self.resource_name, account_id, self.limit)

        self.manager._token_table = mock_token_table
        self.manager._limit_table = mock_limit_table

        self.manager.get_reservation(account_id)

        response = mock_token_table.query(KeyConditionExpression=Key('resourceCoordinate').eq(coordinate))
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

        mock_token_table = create_non_fung_table(self.token_table)
        mock_limit_table = create_limit_table(self.limit_table)
        _insert_limit(mock_limit_table, self.resource_name, account_id, self.limit)

        self.manager._token_table = mock_token_table
        self.manager._limit_table = mock_limit_table

        # Insert enough tokens to reach the limit
        for i in range(0, self.limit):
            token = {
                'resourceCoordinate': coordinate,
                'resourceName': self.resource_name,
                'accountId': account_id,
                'resourceId': 'resource-' + str(i),
                'expirationTime': now + 10000,
                'reservationId': random_string()
            }
            mock_token_table.put_item(Item=token)

        self.assertRaises(CapacityExhaustedException, self.manager.get_reservation, account_id)

    @mock_dynamodb2
    def test_get_token_count(self):
        now = now_utc_sec()
        account_id = random_string()
        coordinate = '{}:{}'.format(self.resource_name, account_id)

        mock_token_table = create_non_fung_table(self.token_table)
        mock_limit_table = create_limit_table(self.limit_table)
        _insert_limit(mock_limit_table, self.resource_name, account_id, self.limit)


        expected_count = 1

        # Insert 2 expired and 1 valid
        expired_token_1 = {
            'resourceCoordinate': coordinate,
            'resourceName': self.resource_name,
            'accountId': account_id,
            'resourceId': random_string(),
            'expirationTime': now,
            'reservationId': random_string()
        }

        expired_token_2 = {
            'resourceCoordinate': coordinate,
            'resourceName': self.resource_name,
            'accountId': account_id,
            'resourceId': random_string(),
            'expirationTime': now - 1000,
            'reservationId': random_string()
        }

        valid_token = {
            'resourceCoordinate': coordinate,
            'resourceName': self.resource_name,
            'accountId': account_id,
            'resourceId': random_string(),
            'expirationTime': now + 300,
            'reservationId': random_string()
        }

        mock_token_table.put_item(Item=expired_token_1)
        mock_token_table.put_item(Item=expired_token_2)
        mock_token_table.put_item(Item=valid_token)

        self.manager._token_table = mock_token_table
        self.manager._limit_table = mock_limit_table
        actual_count = self.manager._get_token_count(account_id, now)

        self.assertEquals(expected_count, actual_count)

    @mock_dynamodb2
    def test_get_token_count_no_tokens(self):
        now = now_utc_sec()
        account_id = random_string()

        mock_token_table = create_non_fung_table(self.token_table)
        mock_limit_table = create_limit_table(self.limit_table)
        _insert_limit(mock_limit_table, self.resource_name, account_id, self.limit)

        self.manager._token_table = mock_token_table
        self.manager._limit_table = mock_limit_table

        expected_count = 0
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
        mock_token_table = create_non_fung_table(self.table_name)

        reserve = TokenReservation(self.res_id, mock_token_table, self.resource_name, self.account_id, self.coordinate)
        _insert_reservation(mock_token_table, reserve)

        response = mock_token_table.query(KeyConditionExpression=Key('resourceCoordinate').eq(self.coordinate))
        self.assertEquals(1, response['Count'])

        reserve.delete()

        response = mock_token_table.query(KeyConditionExpression=Key('resourceCoordinate').eq(self.coordinate))
        self.assertEquals(0, response['Count'])

    def test_create_after_delete(self):
        mock_token_table = Mock()
        mock_token_table.delete_item = Mock()
        mock_token_table.update_item = Mock()
        reserve = TokenReservation(self.res_id, mock_token_table, self.resource_name, self.account_id, self.coordinate)

        reserve.delete()
        self.assertRaises(ValueError, reserve.create_token, random_string())

    def test_double_create(self):
        mock_token_table = Mock()
        mock_token_table.delete_item = Mock()
        mock_token_table.update_item = Mock()
        reserve = TokenReservation(self.res_id, mock_token_table, self.resource_name, self.account_id, self.coordinate)

        reserve.create_token(random_string())
        self.assertRaises(ValueError, reserve.create_token, random_string())

def _insert_reservation(mock_token_table, reservation):
    reservation_item = {
        'resourceCoordinate': reservation.coordinate,
        'resourceName': reservation.resource_name,
        'accountId': reservation.account_id,
        'resourceId': reservation.id,
        'reservationId': reservation.id,
        'expirationTime': now_utc_sec() + 300
    }
    mock_token_table.put_item(Item=reservation_item)

def _insert_limit(mock_limit_table, resource_name, account_id, limit, window_sec=0):
    limit_item = {
        'resourceName': resource_name,
        'accountId': account_id,
        'limit': limit,
        'windowSec': window_sec,
        'serviceName': random_string()
    }
    mock_limit_table.put_item(Item=limit_item)
