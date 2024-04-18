import unicodedata
import html2text
from loguru import logger



def normalize_unicode(text: str | None) -> str | None:
    """
    Normalizes the way the Unicode characters are expressed to the "NFKD" form (https://unicode.org/reports/tr15/).
    Returns `None` if `text` is `None`.
    """
    return unicodedata.normalize('NFKD', text) if text else None

    # Not using the following because it would remove anything not ASCII. It isn't clear this is
    # what we want, but I am keeping it here because it was used during earlier testing phases.
    # return unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode()


def trim_plain_text(text: str | None) -> str | None:
    """
    Trims the specified text and returns `None` if what is left is empty.
    """
    if text:
        text = text.strip()
    return text if text else None


def clean_text(text: str | None) -> str | None:
    markdown_text = html_to_markdown(text)
    trimmed_plain_text = trim_plain_text(markdown_text)
    unicode_normalized_plain_text = normalize_unicode(trimmed_plain_text)
    return unicode_normalized_plain_text


def is_blank(text: str | None) -> bool:
    """
    Checks if the value is empty (""), None or whitespace only

    | is_blank(None) = true
    | is_blank('') = true
    | is_blank('  ') = true
    | is_blank('foo') = false
    | is_blank(' foo ') = false

    :param text: the value to check, may be None
    :return: True if the text is a str, is None, empty or whitespace only
    """
    if isinstance(text, str):
        return not text or text.isspace()
    return True


def html_to_markdown(input_str: str):
    if input_str is None:
        return None
    html_to_text = html2text.HTML2Text()
    return html_to_text.handle(input_str)
