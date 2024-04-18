import langcodes

from utils import i18n_utils

RKM_LANGUAGE_TO_IETF_LANGUAGE_TAGS = {
    'English': 'en',
    'Chinese': 'zh-CN',
    'Chinese Traditional': 'zh-TW',
    'French': 'fr',
    'German': 'de',
    'Italian': 'it',
    'Japanese': 'ja',
    'Korean': 'ko',
    'Spanish': 'es',
    'Portuguese': 'pt',
    'Russian': 'ru',
    'Swedish': 'sv',
    'Dutch': 'nl',
    'Finnish': 'fi',
    'Polish': 'pl',
    'Arabic': 'ar',
    'Hebrew': 'he',
    'Norwegian': 'no',
    'Norwegian bokmål': 'nb',
    'Norwegian nynorsk': 'nn',
    'Danish': 'da',
    'Welsh': 'cy',
    'Turkish': 'tr',
    'Thai': 'th',
    'Catalan': 'ca',
    'Greek': 'el',
    'Romanian': 'ro',
    'Czech': 'cs',
    'Ukrainian': 'uk',
}


def from_rkm_language_to_ietf_language_tag(rkm_language: str | None, default_language_tag: str | None):
    """
    Returns the IETF language tag (e.g., "en", "zh-CN") corresponding to the language field in RKM articles.
    Returns :default_default_language_tag: if unable to translate the specified language input.
    """
    if not rkm_language:
        return default_language_tag

    language_tag = RKM_LANGUAGE_TO_IETF_LANGUAGE_TAGS.get(rkm_language)
    if not language_tag:
        try:
            # Try looking up by English name
            language_tag = langcodes.find(rkm_language, language='en').to_tag()
        except LookupError:
            # Try considering the RKM language as some language code e.g. 'en_US'
            language_tag = i18n_utils.standardize_language_tag(rkm_language, default_language_tag)

    return language_tag
