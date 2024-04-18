
def has_callable_attr(o, name: str) -> bool:
    """ Indicates whether the passed object, `o`, has a callable attribute (i.e., method) of the specified name. """
    return callable(getattr(o, name, None))


def int_defaulted_to_none(obj) -> int:
    """
    Returns ``int(obj)`` if ``obj`` isn't ``None``, else returns ``None``.
    """
    return int(obj) if obj is not None else None
