from utils.file_types_utils import ContentType

CHUNK_SIZE: int = 1024  # this is the max that we can use following the advice from o365 library
SUPPORTED_FILES = [ContentType.WORD, ContentType.PDF]
