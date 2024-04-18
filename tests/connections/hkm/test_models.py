import pytest

from connections.hkm.schemas import HkmArticle, HkmArticleTranslation


def create_test_article(knowledge_states: [str]) -> HkmArticle:
    return HkmArticle(
        content_id=123,
        translations=[create_test_translation(knowledge_state) for knowledge_state in knowledge_states],
    )


def create_test_translation(knowledge_state: str) -> HkmArticleTranslation:
    return HkmArticleTranslation(
        knowledge_state=knowledge_state,
        culture='',
        title='',
        issue='',
        environment='',
        resolution='',
        cause='',
        tags=[],
    )


@pytest.mark.parametrize(
    'article,expected',
    [
        (create_test_article([]), False),
        (create_test_article(['draft']), False),
        (create_test_article(['draft', 'published']), False),
        (create_test_article(['published']), True),
        (create_test_article(['published', 'draft']), True),
    ])
def test_hkm_article_is_published_no_translations(article: HkmArticle, expected: bool):
    assert article.is_published() == expected
