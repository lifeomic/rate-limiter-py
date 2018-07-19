#!/usr/bin/env python
from test.utils import random_string
from unittest import TestCase
from mock import Mock, MagicMock, patch

from limiter import rate_limit, fungible_limiter, non_fungible_limiter
from limiter.managers import TokenReservation

class BaseLimiterTest(TestCase):
    def setUp(self):
        self.limit = 10
        self.window = 100
        self.token_table_name = random_string()
        self.limit_table_name = random_string()
        self.resource_name = random_string()

class FungibleTokenLimiterDecoratorTest(BaseLimiterTest):
    def setUp(self):
        super(FungibleTokenLimiterDecoratorTest, self).setUp()
        self.mock_manager = Mock()
        self.mock_manager.get_token = Mock()

    def test_call_account_id_pos(self):
        arg_1 = random_string()
        arg_2 = random_string()
        account_id = random_string()
        account_id_pos = 2

        func_to_limit = Mock()
        limiter = rate_limit(
            self.resource_name,
            self.limit,
            self.window,
            self.token_table_name,
            self.limit_table_name,
            account_id_pos)
        limiter._manager = self.mock_manager

        rate_limited_func = limiter.__call__(func_to_limit)
        rate_limited_func(arg_1, arg_2, account_id)

        self.mock_manager.get_token.assert_called_with(account_id)
        func_to_limit.assert_called_with(arg_1, arg_2, account_id)

    def test_call_account_id_key(self):
        arg_1 = random_string()
        arg_2 = random_string()
        account_id = random_string()
        account_id_key = 'account'

        func_to_limit = MagicMock()
        limiter = rate_limit(
            self.resource_name,
            self.limit,
            self.window,
            self.token_table_name,
            self.limit_table_name,
            account_id_key=account_id_key)
        limiter._manager = self.mock_manager

        rate_limited_func = limiter.__call__(func_to_limit)
        rate_limited_func(arg_1, arg_2, account=account_id)

        self.mock_manager.get_token.assert_called_with(account_id)
        func_to_limit.assert_called_with(arg_1, arg_2, account=account_id)

    def test_manager_config_ctor_params(self):
        limiter = rate_limit(
            self.resource_name,
            self.limit,
            self.window,
            self.token_table_name,
            self.limit_table_name)
        manager = limiter.manager

        self.assertEquals(self.token_table_name, manager.token_table_name)
        self.assertEquals(self.limit_table_name, manager.limit_table_name)
        self.assertEquals(self.resource_name, manager.resource_name)

    def test_manager_config_env_params(self):
        env_vars = {
            'FUNGIBLE_TABLE': str(self.token_table_name),
            'LIMIT_TABLE': str(self.limit_table_name)
        }

        with patch.dict('os.environ', env_vars):
            limiter = rate_limit(self.resource_name, self.limit, self.window)
            manager = limiter.manager

            self.assertEquals(self.token_table_name, manager.token_table_name)
            self.assertEquals(self.limit_table_name, manager.limit_table_name)
            self.assertEquals(self.resource_name, manager.resource_name)

    @patch('limiter.limiters.FungibleTokenManager')
    def test_decoratored_account_id_pos(self, mock_manager_delegate):
        arg_1 = random_string()
        arg_2 = random_string()
        account_id = random_string()

        mock_manager = Mock()
        mock_manager.return_value.get_token = Mock()
        mock_manager_delegate.return_value = mock_manager

        self.assertTrue(self._limited_func_account_id_pos(arg_1, arg_2, account_id))
        mock_manager.get_token.assert_called_with(account_id)

    @patch('limiter.limiters.FungibleTokenManager')
    def test_decoratored_account_id_key(self, mock_manager_delegate):
        arg_1 = random_string()
        arg_2 = random_string()
        account_id = random_string()

        mock_manager = Mock()
        mock_manager.return_value.get_token = Mock()
        mock_manager_delegate.return_value = mock_manager

        self.assertTrue(self._limited_func_account_id_key(arg_1, arg_2, account_id=account_id))
        mock_manager.get_token.assert_called_with(account_id)

    @rate_limit('my-resource', 10, 1, 'my-token-table', 'my-limit-table', account_id_pos=3)
    def _limited_func_account_id_pos(self, arg_1, arg_2, account_id):
        self.assertIsNotNone(arg_1)
        self.assertIsNotNone(arg_2)
        self.assertIsNotNone(account_id)
        return True

    @rate_limit('my-resource', 10, 1, 'my-token-table', 'my-limit-table')
    def _limited_func_account_id_key(self, arg_1, arg_2, account_id='my-account'):
        self.assertIsNotNone(arg_1)
        self.assertIsNotNone(arg_2)
        self.assertIsNotNone(account_id)
        return True

class FungibleTokenLimiterContextManagerTest(BaseLimiterTest):
    def setUp(self):
        super(FungibleTokenLimiterContextManagerTest, self).setUp()
        self.account_id = random_string()

    @patch('limiter.limiters.FungibleTokenManager')
    def test_get_token_ctor_params(self, mock_manager_delegate):
        mock_manager = Mock()
        mock_manager.return_value.get_token = Mock()
        mock_manager_delegate.return_value = mock_manager

        with fungible_limiter(self.resource_name, self.account_id, 10, 1, self.token_table_name, self.limit_table_name):
            mock_manager.get_token.assert_called_with(self.account_id)

    @patch('limiter.limiters.FungibleTokenManager')
    def test_get_token_env_params(self, mock_manager_delegate):
        env_vars = {
            'FUNGIBLE_TABLE': str(self.token_table_name),
            'LIMIT_TABLE': str(self.limit_table_name)
        }

        with patch.dict('os.environ', env_vars):
            mock_manager = Mock()
            mock_manager.return_value.get_token = Mock()
            mock_manager_delegate.return_value = mock_manager

            with fungible_limiter(self.resource_name, self.account_id, 10, 1):
                mock_manager.get_token.assert_called_with(self.account_id)

class NonFungibleTokenLimiterContextManagerTest(BaseLimiterTest):
    def setUp(self):
        super(NonFungibleTokenLimiterContextManagerTest, self).setUp()
        self.account_id = random_string()

    @patch('limiter.limiters.NonFungibleTokenManager')
    def test_get_token_ctor_params(self, mock_manager_delegate):
        mock_manager = Mock()
        empty_reservation = TokenReservation(None, None, None, None, None)
        mock_manager.get_reservation = MagicMock(return_value=empty_reservation)
        mock_manager_delegate.return_value = mock_manager

        with non_fungible_limiter(self.resource_name,
                                  self.account_id,
                                  self.limit,
                                  self.token_table_name,
                                  self.limit_table_name) as reservation:
            mock_manager.get_reservation.assert_called_with(self.account_id)
            self.assertIs(empty_reservation, reservation)

    @patch('limiter.limiters.NonFungibleTokenManager')
    def test_get_token_env_params(self, mock_manager_delegate):
        env_vars = {
            'NON_FUNGIBLE_TABLE': self.token_table_name,
            'LIMIT_TABLE': self.limit_table_name
        }

        with patch.dict('os.environ', env_vars):
            mock_manager = Mock()
            empty_reservation = TokenReservation(None, None, None, None, None)
            mock_manager.get_reservation = MagicMock(return_value=empty_reservation)
            mock_manager_delegate.return_value = mock_manager

            with non_fungible_limiter(self.resource_name, self.account_id, self.limit) as reservation:
                mock_manager.get_reservation.assert_called_with(self.account_id)
                self.assertIs(empty_reservation, reservation)

    @patch('limiter.limiters.NonFungibleTokenManager')
    def test_delete_on_exception(self, mock_manager_delegate):
        mock_manager = Mock()
        mock_reservation = Mock()
        mock_reservation.delete = Mock()

        mock_manager.get_reservation = MagicMock(return_value=mock_reservation)
        mock_manager_delegate.return_value = mock_manager

        try:
            with non_fungible_limiter(self.resource_name,
                                      self.account_id,
                                      self.limit,
                                      self.token_table_name,
                                      self.limit_table_name) as reservation:
                self.assertNotNone(reservation)
                raise StandardError('Deliberately thrown from test_delete_on_exception')
        except StandardError:
            pass

        mock_reservation.delete.assert_called()
