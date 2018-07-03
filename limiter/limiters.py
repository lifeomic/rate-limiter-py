#!/usr/bin/env python
import os
from managers import FungibleTokenManager

class BaseFungibleTokenLimiter(object):
    def __init__(self, resource_name, table_name=None, limit=None, window=None):
        self.resource_name = resource_name

        self.table_name = self._validate_required_env_fallback(table_name, 'table_name', 'FUNG_TABLE_NAME')
        self.limit = int(self._validate_required_env_fallback(limit, 'limit', 'FUNG_LIMIT'))
        self.window = int(self._validate_required_env_fallback(window, 'window', 'FUNG_WINDOW'))

        self._manager = None

    @property
    def manager(self):
        if not self._manager:
            self._manager = FungibleTokenManager(self.table_name, self.resource_name, self.limit, self.window)
        return self._manager

    def _validate_required_env_fallback(self, param_value, param_name, env_var):
        if param_value:
            return param_value
        if env_var in os.environ:
            return os.environ[env_var]
        raise ValueError(param_name +
            ' must be explicity passed to the decorator constructor, or set in the environment as ' + env_var)

class FungibleTokenContextManager(BaseFungibleTokenLimiter):
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
            account_id = kwargs[self.account_id_index] if self.is_account_id_kwarg else args[self.account_id_index]
            self.manager.get_token(account_id)
            return func_to_limit(*args, **kwargs)
        return rate_limited_func

context_manager = FungibleTokenContextManager
decorator = FungibleTokenLimiterDecorator
