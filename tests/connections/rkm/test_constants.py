import pytest

from connections.rkm.constants import from_rkm_language_to_ietf_language_tag


@pytest.mark.parametrize(
    'rkm_language,language_tag', [
        ('Chinese', 'zh-CN'),
        ('English', 'en'),
        ('French', 'fr'),
        ('German', 'de'),
        ('Hebrew', 'he'),
        ('Italian', 'it'),
        ('Japanese', 'ja'),
        ('Korean', 'ko'),
        ('Polish', 'pl'),
        ('Portuguese', 'pt'),
        ('Russian', 'ru'),
        ('Spanish', 'es'),
        ('en_US', 'en-US'),
        ('pt-BR', 'pt-BR'),
        ('Tagalog', 'tl'),  # not in the OOB list of RKM languages but recognized by `langcodes`
        (None, 'en'),  # defaulted
        ('', 'en'),  # defaulted
        ('not a supported language', 'en'),  # defaulted
    ]
)
def test_from_rkm_language_to_ietf_language_tag(rkm_language, language_tag):
    assert from_rkm_language_to_ietf_language_tag(rkm_language, 'en') == language_tag