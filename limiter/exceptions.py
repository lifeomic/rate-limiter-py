class CapacityExhaustedException(Exception):
    """ Raised when a token is requested by none are available. """
    pass

class ReservationNotFoundException(Exception):
    """ Raised when the query result for a non-fungible token reservation is empty. """
    pass
