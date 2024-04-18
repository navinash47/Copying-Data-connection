from connections.rkm.models import KnowledgeArticle


def test_from_dict():
    entry = {
        'ArticleForm': 'RKM:CustomArticle',
        'FK_GUID': 'TEST_FK_GUID',
        'ArticleTitle': 'TestArticleTitle',
        'DocID': 'TestDocID',
        'InternalArticleIndication': 'Yes',
        'Company': 'TestCompany',
        'Language': 'Japanese'
    }
    article = KnowledgeArticle.from_dict(entry)
    assert article.form == 'RKM:CustomArticle'
    assert article.fk_guid == 'TEST_FK_GUID'
    assert article.title == 'TestArticleTitle'
    assert article.display_id == 'TestDocID'
    assert article.internal
    assert article.company == 'TestCompany'
    assert article.language == 'Japanese'
