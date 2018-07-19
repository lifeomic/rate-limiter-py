#!/usr/bin/env python
from limiter.utils import validate_table_env_fallback
from limiter.managers import FungibleTokenManager, NonFungibleTokenManager

class BaseTokenLimiter(object):
    """
    Base class for both fungible and non-fungible token limiters.

    Args:
        resource_name (str): Name of the resource being rate-limited.
        default_limit (str): Number of tokens to use if no explicit limit is defined in the limits table.
    """
    def __init__(self, resource_name, default_limit):
        self.resource_name = resource_name
        self.default_limit = default_limit
        self._manager = None

class BaseFungibleTokenLimiter(BaseTokenLimiter):
    """
    Extensions of this class provide the most convient usages for rate limiting implementations, e.g. a decorator

    Args:
        resource_name (str): Name of the resource being rate-limited.
        default_limit (str): Number of tokens to use if no explicit limit is defined in the limits table.
        default_window (str): Sliding window of time, in sec, if no explicit limit is defined in the limits table.
        token_table (str): Name of the DynamoDB table containing tokens.
                           Can be set via environment variable `FUNGIBLE_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
        limit_table (str): Name of the DynamoDB table containing limits.
                           Can be set via environment variable `LIMIT_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
    """
    def __init__(self, resource_name, default_limit, default_window, token_table=None, limit_table=None):
        super(BaseFungibleTokenLimiter, self).__init__(resource_name, default_limit)

        self.token_table = validate_table_env_fallback(token_table, 'FUNGIBLE_TABLE', 'fungible-tokens')
        self.limit_table = validate_table_env_fallback(limit_table, 'LIMIT_TABLE', 'limits')
        self.default_window = default_window

    @property
    def manager(self):
        """ Fungible token manager """
        if not self._manager:
            self._manager = FungibleTokenManager(self.token_table,
                                                 self.limit_table,
                                                 self.resource_name,
                                                 self.default_limit,
                                                 self.default_window)
        return self._manager

class FungibleTokenContextManager(BaseFungibleTokenLimiter):
    """
    Fungible token rate-limiter implemented as a context manager.

    Args:
        resource_name (str): Name of the resource being rate-limited.
        account_id (str): The account to retrieve a token on behalf of.
        default_limit (str): Number of tokens to use if no explicit limit is defined in the limits table.
        default_window (str): Sliding window of time, in sec, if no explicit limit is defined in the limits table.
        token_table (str): Name of the DynamoDB table containing tokens.
                           Can be set via environment variable `FUNGIBLE_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
        limit_table (str): Name of the DynamoDB table containing limits.
                           Can be set via environment variable `LIMIT_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.

    Note:
        This class is exported with the more succinct name `fungible_limiter`

    Examples:
        This example assumes `token_table` and `limit_table` have been set via environment variables.

        >>> from limiter import fungible_limiter
        >>> with fungible_limiter('my-resource', 'my-account', 10, 1):
        ...   my_rate_limited_func()
        ...   print 'Done!'
        Done!
    """
    def __init__(self,
                 resource_name,
                 account_id,
                 default_limit,
                 default_window,
                 token_table=None,
                 limit_table=None):
        super(FungibleTokenContextManager, self).__init__(resource_name, default_limit, default_window, token_table,
                                                          limit_table)
        self.account_id = account_id

    def __enter__(self):
        self.get_token()

    def __exit__(self, *args):
        pass

    def get_token(self):
        """ Check the limit and claim a token """
        self.manager.get_token(self.account_id)

class FungibleTokenLimiterDecorator(BaseFungibleTokenLimiter):
    """
    Fungible token rate-limiter implemented as a decorator.

    This decorator requires the account id to be an arguement (positional or keyword) of the function being
    decorated. The location of the account id arguement is set either with `account_id_pos` or `account_id_key`.

    Note:
        This class is exported with the more succinct name `rate_limit`.

    Args:
        resource_name (str): Name of the resource being rate-limited.
        default_limit (str): Number of tokens to use if no explicit limit is defined in the limits table.
        default_window (str): Sliding window of time, in sec, if no explicit limit is defined in the limits table.
        token_table (str): Name of the DynamoDB table containing tokens.
                           Can be set via environment variable `FUNGIBLE_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
        limit_table (str): Name of the DynamoDB table containing limits.
                           Can be set via environment variable `LIMIT_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
        account_id_pos (int): Index of the account id in the args of the function being decorated. Defaults to None.
        account_id_key (str): Key of the account id in the kwargs of the function being decorated.
                              Defaults to `account_id`.

    Examples:
        These examples assume `token_table` and `limit_table` have been set via environment variables.

        >>> from limiter import rate_limit
        >>>
        >>> @rate_limit('my-resource', 10, 1, account_id_pos=1)
        ... def first_func(arg_1, account_id)
        ...   print 'In first_func'
        >>>
        >>> @rate_limit('my-resource', 10, 1, account_id_key=my_account_id)
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
                 default_limit,
                 default_window,
                 token_table=None,
                 limit_table=None,
                 account_id_pos=None,
                 account_id_key='account_id'):
        super(FungibleTokenLimiterDecorator, self).__init__(resource_name, default_limit, default_window, token_table,
                                                            limit_table)
        self.is_account_id_kwarg = account_id_pos is None
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

class NonFungibleTokenLimiterContextManager(BaseTokenLimiter):
    """
    Non-fungible token rate-limiter implemented as a context manager.

    This class does not create tokens. Rather, it creates instances of TokenReservation which are able to create
    a single token. If the context is exited due to an exception, the token reservation will be deleted and the
    exception propogated.

    Args:
        resource_name (str): Name of the resource being rate-limited.
        default_limit (str): Number of tokens to use if no explicit limit is defined in the limits table.
        token_table (str): Name of the DynamoDB table containing tokens.
                           Can be set via environment variable `FUNGIBLE_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.
        limit_table (str): Name of the DynamoDB table containing limits.
                           Can be set via environment variable `LIMIT_TABLE`
                           or synthesized using the `LIMITER_TABLES_BASE_NAME` environment variable.

    Note:
        This class is exported with the more succinct name `non_fungible_limiter`

    Examples:
        This example assumes `token_table` and `limit_table` have been set via environment variables.

        >>> from limiter import non_fungible_limiter
        >>> with non_fungible_limiter('my-resource', 'my-account', 10) as reservation:
        ...   emr_cluster_id = create_emr_cluster()
        ...   reservation.create_token(emr_cluster_id)
        ...   print 'Done!'
        Done!
    """
    def __init__(self,
                 resource_name,
                 account_id,
                 default_limit,
                 token_table=None,
                 limit_table=None):
        super(NonFungibleTokenLimiterContextManager, self).__init__(resource_name, default_limit)

        self.account_id = account_id
        self.token_table = validate_table_env_fallback(token_table, 'NON_FUNGIBLE_TABLE', 'non-fungible-tokens')
        self.limit_table = validate_table_env_fallback(limit_table, 'LIMIT_TABLE', 'limits')

        self.reservation = None

    @property
    def manager(self):
        """ Non-fungible token manager """
        if not self._manager:
            self._manager = NonFungibleTokenManager(self.token_table, self.limit_table, self.resource_name,
                                                    self.default_limit)
        return self._manager

    def __enter__(self):
        self.reservation = self.get_reservation()
        return self.reservation

    def __exit__(self, *args):
        if any(args):
            print str(args)
            self.reservation.delete()

    def get_reservation(self):
        """ Check the limit and create a reservation """
        return self.manager.get_reservation(self.account_id)

non_fungible_context_manager = NonFungibleTokenLimiterContextManager
fungible_context_manager = FungibleTokenContextManager
decorator = FungibleTokenLimiterDecorator
