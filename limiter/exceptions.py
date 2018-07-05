class CapacityExhaustedException(Exception):
    """ Raised when a token is requested by none are available. """
    pass

class ReservationNotFoundException(Exception):
    pass
