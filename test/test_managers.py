#!/usr/bin/env python
import sys
import time
import utils

from limiter import clients
from limiter.managers import FungibleTokenManager
from limiter.exceptions import CapacityExhaustedException
from botocore.exceptions import ClientError
from unittest import TestCase
from mock import Mock, MagicMock

class FungibleTokenManagerTest(TestCase):
    def setUp(self):
        self.table = utils.random_string()
        self.resource_name = utils.random_string()
        self.limit = 10
        self.window = 100
        self.manager = FungibleTokenManager(self.table, self.resource_name, self.limit, self.window)

    def test_compute_refill_amount(self):
        current_tokens = 5
        last_refill = 1530111500
        exec_time = last_refill + 30

        expected = 8
        actual = self.manager._compute_refill_amount(current_tokens, last_refill, exec_time)
        self.assertEquals(expected, actual)

    def test_compute_refill_amount_negative_balance(self):
        current_tokens = -7
        last_refill = 1530111500
        exec_time = last_refill + 30

        expected = 3
        actual = self.manager._compute_refill_amount(current_tokens, last_refill, exec_time)
        self.assertEquals(expected, actual)

    def test_compute_refill_amount_refill_lag(self):
        current_tokens = 0
        last_refill = 1530100000
        exec_time = last_refill + 11500

        expected = self.limit - 1
        actual = self.manager._compute_refill_amount(current_tokens, last_refill, exec_time)
        self.assertEquals(expected, actual)

    def test_get_bucket_token(self):
        account_id = utils.random_string()
        exec_time = int(time.time())

        expected = {'tokens': 5, 'last_refill': int(time.time())}
        response = {'Attributes': expected}
        mock_table = Mock()
        mock_table.update_item = MagicMock(return_value=response)
        self.manager._table = mock_table

        actual = self.manager._get_bucket_token(account_id, exec_time)
        self.assertEquals(expected, actual)

        actual_args = mock_table.update_item.call_args_list
        self.assertEquals(1, len(actual_args))

        expected_args = [
            ({'Key': {
                'resourceName': self.resource_name,
                'accountId': account_id
            },
            'UpdateExpression': 'add tokens :dec',
            'ConditionExpression': 'tokens > :min OR lastRefill < :failsafe OR attribute_not_exists(tokens)',
            'ExpressionAttributeValues': {
                ':dec': -1,
                ':min': 0,
                ':failsafe': exec_time - self.manager.window
            },
            'ReturnValues': 'ALL_NEW'})
        ]

        self.assertEquals(expected_args, actual_args[0])

    def test_get_bucket_token_exhausted(self):
        account_id = utils.random_string()
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
        account_id = utils.random_string()
        tokens = 8
        refill_time = int(time.time())

        mock_table = Mock()
        mock_table.update_item = Mock()
        self.manager._table = mock_table

        self.manager._refill_bucket_tokens(account_id, tokens, refill_time)

        actual_args = mock_table.update_item.call_args_list
        self.assertEquals(1, len(actual_args))

        expected_args = [
            ({'Key': {
                'resourceName': self.resource_name,
                'accountId': account_id
            },
            'UpdateExpression': 'set tokens = :tokens, lastRefill = :refill_time',
            'ConditionExpression': 'lastRefill < :refill_time',
            'ExpressionAttributeValues': {
                ':tokens': tokens,
                ':refill_time': refill_time
            },
            'ReturnValues': 'NONE'})
        ]

        self.assertEquals(expected_args, actual_args[0])
