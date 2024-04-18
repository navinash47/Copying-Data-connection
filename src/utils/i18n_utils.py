import langcodes
from loguru import logger


def standardize_language_tag(language_tag: str | None, default_language_tag: str | None) -> str:
    """
    Wrapper around `langcodes.standardize_tag()`.
    :return: the standardized tag or `default_language_tag` if `language_tag` is not recognized.
             Returns `None` if `language_tag` is `None`.
    """
    if language_tag is None:
        return default_language_tag

    try:
        return langcodes.standardize_tag(language_tag)
    except langcodes.LanguageTagError:
        logger.debug("unknown language: {rkm_language}", rkm_language=language_tag)
        return default_language_tag
