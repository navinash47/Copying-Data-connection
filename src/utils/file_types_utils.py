from types import SimpleNamespace

ContentType = SimpleNamespace()
ContentType.WORD = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
ContentType.PDF = "application/pdf"
ContentType.TEXT_PLAIN = "text/plain"
ContentType.APPLICATION_JSON = 'application/json'


SUPPORTED_CONTENT_TYPES = [ContentType.WORD, ContentType.PDF, ContentType.TEXT_PLAIN]
