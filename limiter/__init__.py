from .limiters import non_fungible_context_manager, fungible_context_manager, decorator

non_fungible_limiter = non_fungible_context_manager
fungible_limiter = fungible_context_manager
rate_limit = decorator
