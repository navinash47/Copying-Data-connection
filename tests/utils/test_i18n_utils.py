import pytest

from utils import i18n_utils


@pytest.mark.parametrize(
    'language_tag,default_language_tag,expected', [
        (None, None, None),
        (None, 'en', 'en'),
        ('en', None, 'en'),
        ('EN', None, 'en'),
        ('en_US', None, 'en-US'),
        ('en-US', None, 'en-US'),
    ]
)
def test_standardize_language_tag(language_tag, default_language_tag, expected):
    assert i18n_utils.standardize_language_tag(language_tag, default_language_tag) == expected
