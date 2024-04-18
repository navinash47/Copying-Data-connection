from langchain.schema import Document

from indexing.models import DocumentMetadata


def test_to_dict():
    metadata = DocumentMetadata(
        connection_id='CONNECTION_ID',
        datasource='DATASOURCE',
        doc_id='DOC_ID',
        doc_display_id='DOC_DISPLAY_ID',
        source='SOURCE',
        language='LANGUAGE',
        title='TITLE',
        tags=['TAG1', 'TAG2'],
        web_url='WEB_URL',
        internal=True,
        company='COMPANY').to_dict()

    assert metadata['connection_id'] == 'CONNECTION_ID'
    assert metadata['datasource'] == 'DATASOURCE'
    assert metadata['doc_id'] == 'DOC_ID'
    assert metadata['doc_display_id'] == 'DOC_DISPLAY_ID'
    assert metadata['source'] == 'SOURCE'
    assert metadata['language'] == 'LANGUAGE'
    assert metadata['title'] == 'TITLE'
    assert metadata['tags'] == ['TAG1', 'TAG2']
    assert metadata['web_url'] == 'WEB_URL'
    assert metadata['internal']
    assert metadata['company'] == 'COMPANY'


def test_to_dict_with_empty_metadata():
    result = DocumentMetadata().to_dict()
    assert len(result) == 0


def test_apply_to_without_preexisting_metadata():
    document = Document(page_content='PAGE CONTENT')
    DocumentMetadata(
        connection_id='CONNECTION_ID',
        datasource='DATASOURCE',
        doc_id='DOC_ID',
        doc_display_id='DOC_DISPLAY_ID',
        source='SOURCE',
        language='LANGUAGE',
        title='TITLE',
        tags=['TAG1', 'TAG2'],
        web_url='WEB_URL',
        internal=True,
        company='COMPANY').apply_to(document)
    metadata = document.metadata

    assert metadata['connection_id'] == 'CONNECTION_ID'
    assert metadata['datasource'] == 'DATASOURCE'
    assert metadata['doc_id'] == 'DOC_ID'
    assert metadata['doc_display_id'] == 'DOC_DISPLAY_ID'
    assert metadata['source'] == 'SOURCE'
    assert metadata['language'] == 'LANGUAGE'
    assert metadata['title'] == 'TITLE'
    assert metadata['tags'] == ['TAG1', 'TAG2']
    assert metadata['web_url'] == 'WEB_URL'
    assert metadata['internal']
    assert metadata['company'] == 'COMPANY'


def test_apply_to_with_preexisting_metadata():
    document = Document(
        page_content='PAGE CONTENT',
        metadata={
            'connection_id': 'CONNECTION_ID 0',
            'datasource': 'DATASOURCE 0',
            'doc_id': 'DOC_ID 0',
            'doc_display_id': 'DOC_DISPLAY_ID 0',
            'source': 'SOURCE 0',
            'language': 'LANGUAGE 0',
            'title': 'TITLE 0',
            'tags': ['TAG0'],
            'web_url': 'WEB_URL 0',
            'internal': False,
            'company': 'COMPANY 0',
            'other_metadata': 'OTHER_METADATA'
        }
    )
    DocumentMetadata(
        connection_id='CONNECTION_ID',
        datasource='DATASOURCE',
        doc_id='DOC_ID',
        doc_display_id='DOC_DISPLAY_ID',
        source='SOURCE',
        language='LANGUAGE',
        title='TITLE',
        tags=['TAG1', 'TAG2'],
        web_url='WEB_URL',
        internal=True,
        company='COMPANY').apply_to(document)
    metadata = document.metadata

    assert metadata['connection_id'] == 'CONNECTION_ID'
    assert metadata['datasource'] == 'DATASOURCE'
    assert metadata['doc_id'] == 'DOC_ID'
    assert metadata['doc_display_id'] == 'DOC_DISPLAY_ID'
    assert metadata['source'] == 'SOURCE'
    assert metadata['language'] == 'LANGUAGE'
    assert metadata['title'] == 'TITLE'
    assert metadata['tags'] == ['TAG1', 'TAG2']
    assert metadata['web_url'] == 'WEB_URL'
    assert metadata['internal']
    assert metadata['company'] == 'COMPANY'
    assert metadata['other_metadata'] == 'OTHER_METADATA'
