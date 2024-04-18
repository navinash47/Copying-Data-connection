from typing import Dict, Iterable


def copy_dict_without_none_values(d: Dict) -> Dict:
    """ Returns a clone of `d` without the entries whose values are None. """
    return {k: v for k, v in d.items() if v is not None}


def join_int_iterable(iterable: Iterable[int] | None, separator: str) -> str | None:
    """ Joins an iterable of ints with the given separator. Returns None if the separator is None. """
    return separator.join([str(field_id) for field_id in iterable]) if iterable is not None else None
