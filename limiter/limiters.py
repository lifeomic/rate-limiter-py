#!/usr/bin/env python
import os
from limiter.managers import FungibleTokenManager

class BaseFungibleTokenLimiter(object):
    """
    Extensions of this class provide the most convient usages for rate limiting implementations, e.g. a decorator

    Args:
        resource_name (str): Name of the resource being rate-limited.
        table_name (str): Name of the DynamoDB table.
                          Can be set via environment variable `FUNG_TABLE_NAME`. Defaults to None.
        limit (int): The maximum number of tokens that may be available.
                     Can be set via environment variable `FUNG_LIMIT`. Defaults to None.
        window (int): Sliding window of time, in seconds, wherein only the `limit` number of tokens will be available.
                      Can be set via environment variable `FUNG_WINDOW`. Defaults to None.
    """
    def __init__(self, resource_name, table_name=None, limit=None, window=None):
        self.resource_name = resource_name

        self.table_name = self._validate_required_env_fallback(table_name, 'table_name', 'FUNG_TABLE_NAME')
        self.limit = int(self._validate_required_env_fallback(limit, 'limit', 'FUNG_LIMIT'))
        self.window = int(self._validate_required_env_fallback(window, 'window', 'FUNG_WINDOW'))

        self._manager = None

    @property
    def manager(self):
        """ Fungible token manager """
        if not self._manager:
            self._manager = FungibleTokenManager(self.table_name, self.resource_name, self.limit, self.window)
        return self._manager

    def _validate_required_env_fallback(self, param_value, param_name, env_var):
        """
        Verify a required argument has a non-null value or has been set via an environment variable.

        Args:
          param_value (obj): Check if this value is non-null.
          param_name (str): Name of the value being checked.
          env_var (str): Name of the environment variable to fallback on.

        Returns:
            obj: `param_value` if it is non-null or the environment variable value.

        Raises:
            ValueError: If `param_value` is null and the environment variable has not been set.
        """
        if param_value:
            return param_value
        if env_var in os.environ:
            return os.environ[env_var]

        msg_format = 'Failed to create limiter for {}. {} must be passed to the decorator or set environment var: {}'
        raise ValueError(msg_format.format(self.resource_name, param_name, env_var))

class FungibleTokenContextManager(BaseFungibleTokenLimiter):
    """
    Fungible token rate-limiter implemented as a context manager.

    Args:
        resource_name (str): Name of the resource being rate-limited.
        account_id (str): The account to retrieve a token on behalf of.
        table_name (str): Name of the DynamoDB table.
                          Can be set via environment variable `FUNG_TABLE_NAME`. Defaults to None.
        limit (int): The maximum number of tokens that may be available.
                     Can be set via environment variable `FUNG_LIMIT`. Defaults to None.
        window (int): Sliding window of time, in seconds, wherein only the `limit` number of tokens will be available.
                      Can be set via environment variable `FUNG_WINDOW`. Defaults to None.

    Note:
        This class is exported with the more succinct name `fungible_limiter`

    Examples:
        This example assumes `table_name`, `limit` and `window` have been set via environment variables.

        >>> from limiter import fungible_limiter
        >>> with fungible_limiter('my-resource', 'my-account'):
        ...   my_rate_limited_func()
        ...   print 'Done!'
        Done!
    """
    def __init__(self,
                 resource_name,
                 account_id,
                 table_name=None,
                 limit=None,
                 window=None):
        super(FungibleTokenContextManager, self).__init__(resource_name, table_name, limit, window)
        self.account_id = account_id

    def __enter__(self):
        self.manager.get_token(self.account_id)

    def __exit__(self, *args):
        pass

class FungibleTokenLimiterDecorator(BaseFungibleTokenLimiter):
    """
    Fungible token rate-limiter implemented as a decorator.

    This decorator requires the account id to be an arguement (positional or keyword) of the function being
    decorated. The location of the account id arguement is set either with `account_id_pos` or `account_id_key`.

    Note:
        This class is exported with the more succinct name `rate_limit`.

    Args:
        resource_name (str): Name of the resource being rate-limited.
        table_name (str): Name of the DynamoDB table.
                          Can be set via environment variable `FUNG_TABLE_NAME`. Defaults to None.
        limit (int): The maximum number of tokens that may be available.
                     Can be set via environment variable `FUNG_LIMIT`. Defaults to None.
        window (int): Sliding window of time, in seconds, wherein only the `limit` number of tokens will be available.
                      Can be set via environment variable `FUNG_WINDOW`. Defaults to None.
        account_id_pos (int): Index of the account id in the args of the function being decorated. Defaults to None.
        account_id_key (str): Key of the account id in the kwargs of the function being decorated.
                              Defaults to `account_id`.

    Examples:
        These examples assume `table_name`, `limit` and `window` have been set via environment variables.

        >>> from limiter import rate_limit
        >>>
        >>> @rate_limit('my-resource', account_id_pos=1)
        ... def first_func(arg_1, account_id)
        ...   print 'In first_func'
        >>>
        >>> @rate_limit('my-resource', account_id_key=my_account_id)
        ... def second_func(arg_1, my_account_id='my-account')
        ...   print 'In second_func'
        >>>
        >>> first_func('foo', 'my-account')
        In first_func
        >>> second_func('bar', 'my-account')
        In second_func
    """
    def __init__(self,
                 resource_name,
                 table_name=None,
                 limit=None,
                 window=None,
                 account_id_pos=None,
                 account_id_key='account_id'):
        super(FungibleTokenLimiterDecorator, self).__init__(resource_name, table_name, limit, window)

        self.is_account_id_kwarg = not account_id_pos
        self.account_id_index = account_id_key if self.is_account_id_kwarg else account_id_pos

    def __call__(self, func_to_limit):
        def rate_limited_func(*args, **kwargs):
            """
            Extract the account id from the decorated function arguments, fetch a token and call the decorated function.

            Args:
              *args (list): Positional arguments for the decorated function.
              **kwargs (dict): Keyword arguments for the decorated function.

            Returns:
                function: The decorated function wrapped in the account id extraction and token retrieval steps.
            """
            account_id = kwargs[self.account_id_index] if self.is_account_id_kwarg else args[self.account_id_index]
            self.manager.get_token(account_id)
            return func_to_limit(*args, **kwargs)
        return rate_limited_func

context_manager = FungibleTokenContextManager
decorator = FungibleTokenLimiterDecorator
