from typing import Iterable, List

from langchain.schema import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter


def generate_chunks(document: Iterable[Document], chunk_size: int = 500, chunk_overlap: int = 100) -> List[Document]:
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    return text_splitter.split_documents(document)
