#!/bin/bash/env python
import logging
import uuid
from datetime import datetime
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from limiter.clients import dynamodb
from limiter.exceptions import CapacityExhaustedException, ReservationNotFoundException

logger = logging.getLogger()

# Dynamo table columns
RESOURCE_NAME = 'resourceName'
ACCOUNT_ID = 'accountId'
TOKENS = 'tokens'
LAST_REFILL = 'lastRefill'
RESOURCE_COORDINATE = 'resourceCoordinate'
RESOURCE_ID = 'resourceId'
EXPIRATION_TIME = 'expirationTime'

class BaseTokenManager(object):
    """
    Base class for both fungible and non-fungible token managers.

    Args:
        table_name (str): Name of the DynamoDB table.
        resource_name (str): Name of the resource being rate-limited.
        limit (int): The maximum number of tokens that may be available.
    """
    def __init__(self, table_name, resource_name, limit):
        self.table_name = table_name
        self.resource_name = resource_name
        self.limit = limit

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

class FungibleTokenManager(BaseTokenManager):
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
        super(FungibleTokenManager, self).__init__(table_name, resource_name, limit)
        self.window = window
        self.tokens_sec = float(limit) / window # Number of tokens the bucket will accumulate per second

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

        exec_time = now_utc_sec()
        bucket = self._get_bucket_token(account_id, exec_time)

        current_tokens = bucket[TOKENS]
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
            update_exp = 'add {} :dec'.format(TOKENS)
            condition_exp = '{0} > :min OR {1} < :failsafe OR attribute_not_exists({0})'.format(TOKENS, LAST_REFILL)
            return self.table.update_item(
                Key={
                    RESOURCE_NAME: self.resource_name,
                    ACCOUNT_ID: account_id
                },
                UpdateExpression=update_exp,
                ConditionExpression=condition_exp,
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
            update_exp = 'set {} = :tokens, {} = :refill_time'.format(TOKENS, LAST_REFILL)
            condition_exp = '{} < :refill_time'.format(LAST_REFILL)
            self.table.update_item(
                Key={
                    RESOURCE_NAME: self.resource_name,
                    ACCOUNT_ID: account_id
                },
                UpdateExpression=update_exp,
                ConditionExpression=condition_exp,
                ExpressionAttributeValues={
                    ':tokens': tokens,
                    ':refill_time': refill_time
                },
                ReturnValues='NONE'
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                logger.warn('Failed to refill tokens for %s:%s, someone else already refilled with more current state',
                            self.resource_name, account_id)
            else:
                raise

class NonFungibleTokenManager(BaseTokenManager):
    """
    Creates reservations for and enforces limits on non-fungible tokens for a single resource stored in DynamoDB.

    Unlike FungibleTokenManager, this class does not create tokens. Rather, it creates instances of
    TokenReservation, a placeholder capable of creating a fully constructed token. Both reservations and complete
    tokens count towards the resource limit. When a reservation is created, it is given a TTL of 300 seconds,
    the maximum execution time of a lambda.

    Args:
        table_name (str): Name of the DynamoDB table.
        resource_name (str): Name of the resource being rate-limited.
        limit (int): The maximum number of tokens that may be available.
    """

    def get_reservation(self, account_id):
        """
        Check the token limit and create a TokenReservation.

        Args:
            account_id (str): The account to retrieve a token on behalf of.

        Returns:
            TokenReservation: A reservation to create a token.

        Raises:
            CapacityExhaustedException: If the number of tokens and reservations are at the resource limit.
        """
        exec_time = now_utc_sec()
        if self._get_token_count(account_id, exec_time) >= self.limit:
            message = 'Resource capcity exhausted for {}:{}'.format(self.resource_name, account_id)
            raise CapacityExhaustedException(message)

        return self._build_reservation(account_id, exec_time)

    def _get_token_count(self, account_id, exec_time):
        """
        Get the number of tokens as reservations associated with this resource and the specified account.

        Note:
            Items in DynamoDB which have exceeded their TTL may still appear in query results. This necessitates
            filtering on expirationTime.

        Args:
            account_id (str): The account to get the number of active tokens/reservations for this resource.
            exec_time (int): Timestamp, in seconds, when the calling operation started.

        Returns:
            int: The number of tokens/reservations for this account on this resource.
        """
        coordinate = self._buid_coordinate(account_id)
        return self.table.query(
            Select='COUNT',
            ConsistentRead=True,
            KeyConditionExpression=Key(RESOURCE_COORDINATE).eq(coordinate),
            FilterExpression=Attr(EXPIRATION_TIME).gt(exec_time)
        )['Count']

    def _build_reservation(self, account_id, exec_time):
        """
        Insert a reservation into DynamoDB and build and instance of TokenReservation.

        Note:
            The reservation will be inserted with a TTL of 300 seconds.

        Args:
            account_id (str): The account to create the reservation on behalf of.
            exec_time (int): Timestamp, in seconds, when the calling operation started.

        Returns:
            TokenReservation: Reservation to create a token on behalf of the specified account on this resource.
        """
        id = str(uuid.uuid4())
        coordinate = self._buid_coordinate(account_id)
        expiration_time = exec_time + 300

        self.table.put_item(
            Item={
                RESOURCE_COORDINATE: coordinate,
                RESOURCE_NAME: self.resource_name,
                ACCOUNT_ID: account_id,
                RESOURCE_ID: id,
                EXPIRATION_TIME: expiration_time
            }
        )
        return TokenReservation(id, self.table, self.resource_name, account_id, coordinate)

    def _buid_coordinate(self, account_id):
        """
        Build a token resource coordinate for this resource and the specified account.

        Args:
            account_id (str): Id of the account to synthesize the coordinate from.

        Returns:
            str: A coordinate value for this resource and the specified account.
        """
        return '{}:{}'.format(self.resource_name, account_id)

class TokenReservation(object):
    """
    Used to represent a temporary placeholder for, and create a non-fungible token, in DynamoDB.

    Instances of this class represent a single placeholder token for a specific resource and account.
    When a fully formed non-fungible token needs to be created from a reservation it will update the entry
    in DynamoDB with the given resource id and extend its TTL.

    Args:
        id (str): Unique id of the reservation. This is used as the resource id until the full token is created.
        table (boto3.Table): Instance of a boto3 DynamoDB table. This table contains the reservation entry.
        resource_name (str): Name of the resource this reservation is on.
        account_id (str): Account this reservation is for.
        coordinate (str): Resource coordinate this reservation is representing.
    """
    def __init__(self, id, table, resource_name, account_id, coordinate):
        self.id = id
        self.table = table
        self.resource_name = resource_name
        self.account_id = account_id
        self.coordinate = coordinate

        self.is_deleted = False
        self.is_token_created = False

    def create_token(self, resource_id, expiration=28800):
        """
        Create a non-fungible token from this reservation.

        The reservation entry in DynamoDB will be updated to use the given resource id as the token resource id
        and extend the TTL.

        Args:
            resource_id (str): Id of the resource this token will represent, e.g. EMR cluster id.
            expiration (int): The token TTL in seconds. Defaults to 28800 (8 hours).

        Raises:
            ValueError: If a token has already been created from this reservation, or this reservation has been deleted.
            ReservationNotFoundException: If the reservation cannot be found in DynamoDB, likely meaning it expired.
        """
        if self.is_token_created:
            raise ValueError('Token already created for {} from this reservation [{}]'.format(resource_id, self.id))

        if self.is_deleted:
            raise ValueError('This reservation [{}] has been deleted'.format(self.id))

        try:
            expiration_time = now_utc_sec() + expiration
            update_exp = 'set {} = :exp_time, set {} = :resource_id'.format(EXPIRATION_TIME, RESOURCE_ID)
            self.table.update_item(
                Key={
                    RESOURCE_COORDINATE: self.coordinate,
                    RESOURCE_ID: self.id
                },
                UpdateExpression=update_exp,
                ExpressionAttributeValues={
                    ':exp_time': expiration_time,
                    ':reserve_id': self.id,
                    ':resource_id': resource_id
                },
                ReturnValues='ALL_NEW'
            )
            self.is_token_created = True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                msg_fmt = 'Reservation {} not found for {}:{}. Possibly expired'
                raise ReservationNotFoundException(msg_fmt.format(self.id, self.resource_name, self.account_id))
            raise

    def delete(self):
        """
        Delete the entry in DynamoDB representing this reservation.
        """
        if self.is_token_created:
            logger.warn('Cannot delete, a token has already been created from this reservation [%s]', self.id)
            return

        if self.is_deleted:
            logger.warn('Cannot delete, this reservation [%s], has already been deleted', self.id)
            return

        self.table.delete_item(
            Key={
                RESOURCE_COORDINATE: self.coordinate,
                RESOURCE_ID: self.id
            },
            ReturnValues='NONE'
        )
        self.is_deleted = True

def now_utc_sec():
    """ Get the number of seconds since the epoch """
    return int(datetime.utcnow().strftime('%s'))
