from typing import List, Dict, Any

from langchain.schema import Document

from utils.collections_utils import copy_dict_without_none_values


class DocumentMetadata:
    """
    Used as a builder or guide to create OpenSearch document metadata.
    Meant to help developers use only existing metadata properties.

    Usage::

        Document(
            page_content='...',
            metadata=DocumentMetadata(doc_id='123').to_dict()
        )

        DocumentMetadata(doc_id='123').merge_into(document)
    """
    def __init__(self,
                 *,  # indicates there are no positional arguments
                 connection_id: str = None,
                 datasource: str = None,
                 doc_id: str = None,
                 doc_display_id: str = None,
                 source: str = None,
                 language: str = None,
                 title: str = None,
                 tags: List[str] = None,
                 web_url: str = None,
                 internal: bool = None,
                 company: str = None):
        self.connection_id = connection_id
        self.doc_id = doc_id
        self.doc_display_id = doc_display_id
        self.source = source
        self.datasource = datasource
        self.title = title
        self.internal = internal
        self.language = language
        self.tags = tags
        self.company = company
        self.web_url = web_url

    def to_dict(self) -> Dict[str, Any]:
        """ Returns a representation of the metadata ready to be passed to OpenSearch. """
        return copy_dict_without_none_values(self.__dict__)

    def apply_to(self, document: Document):
        """
        Updates the metadata of the specified document with the properties of this object.
        Metadata properties of ``document``, which aren't overwritten, are preserved.
        """
        if hasattr(document, 'metadata'):
            document.metadata.update(self.to_dict())
        else:
            document.metadata = self.to_dict()
