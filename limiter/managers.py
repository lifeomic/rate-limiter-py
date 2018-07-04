#!/bin/bash/env python
import logging
import time
from botocore.exceptions import ClientError

from limiter.clients import dynamodb
from limiter.exceptions import CapacityExhaustedException

logger = logging.getLogger()

class FungibleTokenManager(object):
    """
    Consumes, replenishes and enforces limits on fungible tokens for a single resource stored in a DynamoDB table.

    When a token is requested the manager will conditionally decrement the number of tokens, with a failed
    update signaling hitting the rate limit. If the limit is hit, a `CapacityExhaustedException` will be raised.

    One of three conditions must be true for the token count to be decremented.
    1.  The number of tokens is greater than 0.

    2.  The current time is greater than the last refill time, plus the `window`.
        This condition is necessary to ensure clients will still be able to
        access resources when tokens fail to be refilled.

    3.  The `tokens` column does not exist. This signifies the row, or bucket, has not been created yet.

    After the manager successfully obtains a token it will add back the number of tokens accumulated
    since the last refill. Updates will be conditionally applied based on last refill time, with stale updates failing.

    Args:
        table_name (str): Name of the DynamoDB table.
        resource_name (str): Name of the resource being rate-limited.
        limit (int): The maximum number of tokens that may be available.
        window (int): Sliding window of time, in seconds, wherein only the `limit` number of tokens will be available.
    """

    def __init__(self, table_name, resource_name, limit, window):
        self.table_name = table_name
        self.resource_name = resource_name
        self.limit = limit
        self.window = window
        self.tokens_sec = float(limit) / window # Number of tokens the bucket will accumulate per second

        self._client = None
        self._table = None

    @property
    def client(self):
        """ DynamoDB client """
        if not self._client:
            self._client = dynamodb()
        return self._client

    @property
    def table(self):
        """ DynamoDB Table containing token row """
        if not self._table:
            self._table = self.client.Table(self.table_name)
        return self._table

    def get_token(self, account_id):
        """
        Retrieve a token on behalf of the specified account.

        If the account has reached its limit a `CapacityExhaustedException` will be raised.
        If a token was successfully retrieved, the number of tokens accumulated since the last
        refill will be added back to the balance.

        Note:
            This method does not actually return a token. If no exception is thrown it means
            the request was not rate limited.

        Args:
            account_id (str): The account to retrieve a token on behalf of.
        """

        exec_time = int(time.time())
        bucket = self._get_bucket_token(account_id, exec_time)

        current_tokens = bucket['tokens']
        last_refill = int(bucket.get('last_refill', 0)) # If the row has not been created yet, use 0
        refill_tokens = self._compute_refill_amount(current_tokens, last_refill, exec_time)

        self._refill_bucket_tokens(account_id, refill_tokens, exec_time)

    def _get_bucket_token(self, account_id, exec_time):
        """
        Conditionally retrieve a token from and return the current state of the bucket (table row).

        Args:
          account_id (str): The account to retrieve a token on behalf of.
          exec_time (int): Time, in seconds, when the token retrieval started.

        Returns:
            dict: State of the bucket after removing a token.

        Raises:
            CapacityExhaustedException: If no more tokens can be taken.
        """
        try:
            return self.table.update_item(
                Key={
                    'resourceName': self.resource_name,
                    'accountId': account_id
                },
                UpdateExpression='add tokens :dec',
                ConditionExpression='tokens > :min OR lastRefill < :failsafe OR attribute_not_exists(tokens)',
                ExpressionAttributeValues={
                    ':dec': -1,
                    ':min': 0,
                    ':failsafe': exec_time - self.window
                },
                ReturnValues='ALL_NEW'
            )['Attributes']
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                message = 'Resource capcity exhausted for {}:{}'.format(self.resource_name, account_id)
                raise CapacityExhaustedException(message)
            raise

    def _compute_refill_amount(self, current_tokens, last_refill, exec_time):
        """
        Compute the number of tokens accumulated since the last refill.

        Args:
          current_tokens (int): The number of tokens currently in the bucket.
          last_refill (int): Timestamp, in seconds, since the last time the bucket was refilled.
          exec_time: Time, in seconds, when the token retrieval started.

        Returns:
            int: The number of tokens accumulated since the last refill.
        """
        tokens = max(0, current_tokens) # Tokens can be negative on bucket creation or a prolonged failure to refill
        time_since_refill = exec_time - last_refill

        return min(self.limit - 1, tokens + int(self.tokens_sec * time_since_refill))

    def _refill_bucket_tokens(self, account_id, tokens, refill_time):
        """
        Update the token balance to include tokens accumulated since the last refill.

        Args:
          account_id (str): Account which owns the bucket to be refilled.
          tokens (int): The new token balance.
          refill_time (int): Time, in seconds, when the token retrieval started.

        Returns:
            dict: State of the bucket after refilling the tokens, if the refill succeeded, None otherwise.
        """
        try:
            self.table.update_item(
                Key={
                    'resourceName': self.resource_name,
                    'accountId': account_id
                },
                UpdateExpression='set tokens = :tokens, lastRefill = :refill_time',
                ConditionExpression='lastRefill < :refill_time',
                ExpressionAttributeValues={
                    ':tokens': tokens,
                    ':refill_time': refill_time
                },
                ReturnValues='NONE'
            )
        except ClientError as e:
            if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                logger.warn('Failed to refill tokens for %s:%s, someone else already refilled with more current state',
                            self.resource_name, account_id)
            else:
                raise
