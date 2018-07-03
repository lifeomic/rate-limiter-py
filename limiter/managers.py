#!/bin/bash/env python
import logging
import time
import clients

from exceptions import CapacityExhaustedException
from botocore.exceptions import ClientError

logger = logging.getLogger()

'''
Negotiates the retreival and replenishment of fungile tokens.

Instances of this class
'''
class FungibleTokenManager(object):
    def __init__(self, table_name, resource_name, limit, window):
        self.table_name = table_name
        self.resource_name = resource_name
        self.limit = limit
        self.window = window
        self.tokens_sec = float(limit) / window

        self._client = None
        self._table = None

    @property
    def client(self):
        if not self._client:
            self._client = clients.dynamodb()
        return self._client

    @property
    def table(self):
        if not self._table:
            self._table = self.client.Table(self.table_name)
        return self._table

    def get_token(self, account_id):
        exec_time = int(time.time())
        bucket = self._get_bucket_token(account_id, exec_time)

        current_tokens = bucket['tokens']
        last_refill = int(attributes.get('last_refill', 0))
        refill_tokens = self._compute_refill_amount(current_tokens, last_refill, exec_time)

        self._refill_bucket_tokens(resource_name, account_id, refill_tokens, exec_time)

    def _get_bucket_token(self, account_id, exec_time):
        try:
            return self.table.update_item(
                Key = {
                    'resourceName': self.resource_name,
                    'accountId': account_id
                },
                UpdateExpression = 'add tokens :dec',
                ConditionExpression = 'tokens > :min OR lastRefill < :failsafe OR attribute_not_exists(tokens)',
                ExpressionAttributeValues = {
                    ':dec': -1,
                    ':min': 0,
                    ':failsafe': exec_time - self.window
                },
                ReturnValues = 'ALL_NEW'
            )['Attributes']
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise CapacityExhaustedException('Resource capcity exhausted for {}:{}'
                    .format(self.resource_name, account_id))
            raise

    def _compute_refill_amount(self, current_tokens, last_refill, exec_time):
        tokens = max(0, current_tokens)
        last_refill = int(last_refill)

        time_since_refill = exec_time - last_refill
        return min(self.limit - 1, tokens + int(self.tokens_sec * time_since_refill))

    def _refill_bucket_tokens(self, account_id, tokens, refill_time):
        try:
             self.table.update_item(
                Key = {
                    'resourceName': self.resource_name,
                    'accountId': account_id
                },
                UpdateExpression = 'set tokens = :tokens, lastRefill = :refill_time',
                ConditionExpression = 'lastRefill < :refill_time',
                ExpressionAttributeValues = {
                    ':tokens': tokens,
                    ':refill_time': refill_time
                },
                ReturnValues = 'NONE'
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                logger.warn('Failed to refill tokens for %s:%s, someone else already refilled with more current state',
                    self.resource_name, account_id)
            else:
                raise
