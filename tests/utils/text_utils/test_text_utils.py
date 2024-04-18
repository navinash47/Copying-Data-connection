import os

import pytest

from utils.text_utils import is_blank
from utils.text_utils import html_to_markdown
from utils.text_utils import trim_plain_text


@pytest.mark.parametrize('text,expected', [
    (None, True),
    ('', True),
    ('  ', True),
    ('\n', True),
    ('\r', True),
    ('\t', True),
    ('\v', True),
    ('\f', True),
    ('value', False),
    ('  value', False),
    ('value  ', False),
    ('  value  ', False),
    (True, True),
    (100, True),
])
def test_is_blank(text: str, expected: bool):
    assert is_blank(text) is expected


def load_test_data(filename):
    current_dir = os.path.dirname(__file__)
    file_path = os.path.join(current_dir, filename)
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()


@pytest.mark.parametrize('html_file,md_file', [
    ('test_data_html_to_markdown.html', 'test_data_html_to_markdown.md'),
    # Add more file pairs as needed
])
def test_html_to_markdown(html_file, md_file):
    html_text = load_test_data(html_file)
    expected_markdown = load_test_data(md_file)
    assert html_to_markdown(html_text) == expected_markdown


@pytest.mark.parametrize('html_file,md_file', [
    ('test_data_html_to_empty_md.html', 'test_data_html_to_empty_md.md'),
    # Add more file pairs as needed
])
def test_html_to_empty_markdown(html_file, md_file):
    empty_html = load_test_data(html_file)
    empty_markdown = load_test_data(md_file)
    assert html_to_markdown(empty_html) == empty_markdown


def test_html_empty_markdown_none():
    return_value = html_to_markdown(None)
    assert return_value is None


@pytest.mark.parametrize('markdown, plain_text', [
    ('test_data_trim_plain_text.md', 'test_data_trim_plain_text.txt'),
])
def test_trim_plain_text(markdown, plain_text):
    markdown_text = load_test_data(markdown)
    expected_plain_text = load_test_data(plain_text)
    assert trim_plain_text(markdown_text) == expected_plain_text
