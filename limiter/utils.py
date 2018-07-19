#!/usr/bin/env python
import os

TABLE_BASE_ENV_VAR = 'LIMITER_TABLES_BASE_NAME'

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
