#!/usr/bin/env python
import json
from boto3.dynamodb.conditions import Key
from limiter.utils import validate_table_env_fallback
from limiter.clients import dynamodb
from limiter.managers import ACCOUNT_ID, RESOURCE_NAME, LIMIT, WINDOW_SEC

# DynamoDB table columns
SERVICE_NAME = 'serviceName'
CONFIG_VERSION = 'configVersion'

class LimitLoader(object):
    """
    Performs initial limit loading and updates for a specified service.

    Instances of this class will only perform a single successful limit load. Subsequent
    calls to load limits will exit without side effects.

    Args:
        service (str): Name of the service limits are being loaded for.
        limit_table (str): Name of the DynamoDB table containing limits.
                           Can be set via environment variable `LIMIT_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
        limit_service_index (str): Name of the DynamoDB limit table service index.
                                   Can be set via environment variable `LIMIT_SERVICE_INDEX`
                                   or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
    """
    def __init__(self, service, limit_table=None, limit_service_index=None):
        self.service = service
        self.limit_table_name = validate_table_env_fallback(limit_table, 'LIMIT_TABLE', 'limits')
        self.service_index_name = validate_table_env_fallback(limit_service_index,
                                                              'LIMIT_SERVICE_INDEX', 'limits-service-index')
        self.limit_table = dynamodb().Table(self.limit_table_name)

        self.is_loaded = False

    def load_limits(self, file_path, force=False):
        """
        Update the limits table entries for the given service with those contained in the specified file.

        First, the latest limits are read from the specified JSON file. Next, the limits table is queried for
        limits for the specified service. Any queried limits which are not in the latset limits will be deleted.
        Any queried limits which do not match the latest limits are updated. Any new limits are published.

        Args:
            service (str): Name of the service limits are being loaded for.
            force (bool): Update the limits even if a successful update has already occurred. Defaults to False.
        """
        if self.is_loaded and not force:
            return

        # Get latest limits
        with open(file_path, 'r') as limits_file:
            limits_json = json.load(limits_file)
        latest_limits = limits_json['limits']

        if not latest_limits:
            self.is_loaded = True
            return

        current_limits_response = self._get_current_limits()

        # Index the latest limits by accound and resource to make diffing easier
        indexed_put_requests = {}
        for limit in latest_limits:
            key, item = self._build_put_item(limit)
            indexed_put_requests[key] = item

        # Update limits
        with self.limit_table.batch_writer() as batch:
            for curr_limit in current_limits_response['Items']:
                account_id = curr_limit[ACCOUNT_ID]
                resource_name = curr_limit[RESOURCE_NAME]
                key = account_id + resource_name

                # Current limit is not in the latest set of limits, delete
                if key not in indexed_put_requests:
                    batch.delete_item(Key={
                        ACCOUNT_ID: account_id,
                        RESOURCE_NAME: resource_name
                    })
                    continue

                # If the limit and window are the same remove from from the dict so its not updated
                put_request = indexed_put_requests[key]
                if curr_limit[LIMIT] == put_request[LIMIT] and curr_limit[WINDOW_SEC] == put_request[WINDOW_SEC]:
                    del indexed_put_requests[key]

            # Update out of sync limits and create new ones
            for item in indexed_put_requests.values():
                batch.put_item(item)

        self.is_loaded = True

    def _get_current_limits(self):
        """
        Fetch the current service limits.

        Returns:
            (dict): Query response containing the current service limits.
        """
        return self.limit_table.query(
            IndexName=self.service_index_name,
            KeyConditionExpression=Key(SERVICE_NAME).eq(self.service)
        )

    def _build_put_item(self, limit):
        """
        Build the limit table item and account/resource key.

        Args:
            limit (dict): Limit to create a table item from. Expected to be pulled from the limits JSON file.

        Returns:
            (tuple): Two element tuple. First element is accound_id + resource_name str. Second is the table item.
        """
        account_id = limit[ACCOUNT_ID]
        resource_name = limit[RESOURCE_NAME]

        item = {
            ACCOUNT_ID: account_id,
            RESOURCE_NAME: resource_name,
            LIMIT: limit[LIMIT],
            WINDOW_SEC: limit.get(WINDOW_SEC, 0),
            SERVICE_NAME: self.service
        }
        key = account_id + resource_name
        return key, item
