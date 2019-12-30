class CapacityExhaustedException(Exception):
    """ Raised when a token is requested but none are available. """

class ReservationNotFoundException(Exception):
    """ Raised when the query result for a non-fungible token reservation is empty. """

class ThrottlingException(Exception):
    """ Raised when the limiter is throttled by AWS. """

class RateLimiterException(Exception):
    """ Raised by a limiter on unrecoverable errors when fetching a token or account limits. """
