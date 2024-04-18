import pytest

from utils.object_utils import int_defaulted_to_none


@pytest.mark.parametrize('obj,expected', [
    (None, None),
    (1, 1),
    (1.0, 1),
    ('1', 1)
])
def test_int_defaulted_to_none(obj, expected):
    assert int_defaulted_to_none(obj) == expected
