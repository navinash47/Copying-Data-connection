from langchain.document_loaders import PyPDFLoader, Docx2txtLoader, TextLoader
from langchain.document_loaders.base import BaseLoader

from utils.file_types_utils import ContentType


def get_langchain_loader(content_type: str, file_path: str) -> BaseLoader:
    match content_type:
        case ContentType.PDF:
            return PyPDFLoader(file_path)
        case ContentType.WORD:
            return Docx2txtLoader(file_path)
        case ContentType.TEXT_PLAIN:
            return TextLoader(file_path)
        case _:
            raise ValueError(f'unsupported file content type: {content_type}')