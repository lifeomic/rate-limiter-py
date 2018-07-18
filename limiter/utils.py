#!/usr/bin/env python
import os
import json
from boto3.dynamodb.conditions import Key, Attr
from limiter.clients import dynamodb
from limiter.managers import ACCOUNT_ID, RESOURCE_NAME, LIMIT, WINDOW_SEC

TABLE_BASE_ENV_VAR = 'LIMITER_TABLES_BASE_NAME'

# DynamoDB table columns
SERVICE_NAME = 'serviceName'
CONFIG_VERSION = 'configVersion'

def validate_table_env_fallback(table_var, env_var, post_fix):
    """
    Check if the given table value is set, falling back to environment variables if not.

    If the given table value is not None, return it. If the value is None, return the specified
    environment variable value, if set. Finally, check if the LIMITER_TABLES_BASE_NAME is set,
    building the table name if set.

    Args:
        table_var (str): Table name value to check.
        env_var (str): Name of the environment variable containing the table name.
        post_fix (str): String to append to the end of LIMITER_TABLES_BASE_NAME.

    Returns:
        str: Name of the table, either the given value, value and the specified env variable or a synthesized one.

    Throws:
        ValueError: If the given table value is None, the specified environment variable is not set
                    and LIMITER_TABLES_BASE_NAME is not set.
    """
    if table_var:
        return table_var
    if env_var in os.environ:
        return os.environ[env_var]
    if TABLE_BASE_ENV_VAR in os.environ:
        base = os.environ[TABLE_BASE_ENV_VAR]
        base = base if base.endswith('-') else base + '-'
        return base + post_fix
    raise ValueError('Failed to retrieve a valid table name')

def load_limits(file_path, service, limit_table=None, limit_service_index=None):
    """
    Update the limits table entries for the given service with those contained in the specified file.

    First, the latest limits are read from the specified JSON file. Next, the limits table is queried for
    limits for the specified service, which do not match the latest version. If no out of date limits are found,
    nothing further is done. Any limits found in the table, but are not present in the JSON file are deleted.
    The remaining limits are updated.

    Args:
        file_path (str): Relative path to the JSON file containing the service limits.
        service (str): Name of the service limits are being loaded for.
        limit_table (str): Name of the DynamoDB table containing limits.
                           Can be set via environment variable `LIMIT_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
        limit_service_index (str): Name of the DynamoDB limit table service index.
                                   Can be set via environment variable `LIMIT_SERVICE_INDEX`
                                   or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
    """
    limit_table_name = validate_table_env_fallback(limit_table, 'LIMIT_TABLE', 'limits')
    service_index_name = validate_table_env_fallback(limit_service_index, 'LIMIT_SERVICE_INDEX', 'limits-service-index')
    limit_table = dynamodb().Table(limit_table_name)

    # Get latest limits
    with open(file_path, 'r') as limits_file:
        limits_json = json.load(limits_file)
    version = limits_json['version']
    latest_limits = limits_json['limits']

    # Check we need to update the limits
    outdated_responses = _get_outdated_limits(limit_table, service_index_name, service, version)
    if not outdated_responses['Count']:
        return

    # Index the latest limits by accound and resource to make diffing easier
    indexed_put_requests = {}
    for limit in latest_limits:
        key, item = _build_put_item(limit, service, version)
        indexed_put_requests[key] = item

    # Update limits
    with limit_table.batch_writer() as batch:
        # If there is a limit in Dynamo but not in our latest set, delete it
        for outdated in outdated_responses['Items']:
            account_id = outdated[ACCOUNT_ID]
            resource_name = outdated[RESOURCE_NAME]

            key = account_id + resource_name
            if key not in indexed_put_requests:
                batch.delete_item(Key={
                    ACCOUNT_ID: account_id,
                    RESOURCE_NAME: resource_name
                })

        # Update the limits
        for item in indexed_put_requests.values():
            batch.put_item(item)

def _get_outdated_limits(limit_table, service_index_name, service, version):
    """
    Fetch the service limits which do not have the specified version.

    Args:
        limit_table (boto3.Table): Instance of a boto3 DynamoDB table.
        service_index_name (str): Name of the DynamoDB limit table service index.
        service (str): Name of the service limits are queried for.
        version (int): Latest version of the service limits.

    Returns:
        (dict): Query response containing limit items for the service which do not match the specified version.
    """
    return limit_table.query(
        IndexName=service_index_name,
        KeyConditionExpression=Key(SERVICE_NAME).eq(service),
        FilterExpression=Attr(CONFIG_VERSION).ne(version)
    )

def _build_put_item(limit, service, version):
    """
    Build the limit table item and account/resource key.

    Args:
        limit (dict): Limit to create a table item from. Expected to be pulled from the limits JSON file.
        service (str): Name of the service the limit is on.
        version (int): Version of the limit.

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
        SERVICE_NAME: service,
        CONFIG_VERSION: version
    }
    key = account_id + resource_name
    return key, item
