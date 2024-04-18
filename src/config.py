from pydantic import BaseSettings


class Settings(BaseSettings):
    """
    Exposes the global launch settings of the application. Reads values from the env environments or from a `.env`
    configuration file.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.RKM_URL = self.RKM_URL or self.INNOVATION_SUITE_URL
        self.RKM_USER = self.RKM_USER or self.INNOVATION_SUITE_USER
        self.RKM_PASSWORD = self.RKM_PASSWORD or self.INNOVATION_SUITE_PASSWORD
        self.RKM_VERIFY_CERTIFICATES = self.RKM_VERIFY_CERTIFICATES or self.INNOVATION_SUITE_VERIFY_CERTIFICATES

        self.HKM_URL = self.HKM_URL or self.INNOVATION_SUITE_URL
        self.HKM_USER = self.HKM_USER or self.INNOVATION_SUITE_USER
        self.HKM_PASSWORD = self.HKM_PASSWORD or self.INNOVATION_SUITE_PASSWORD

        self.BWF_URL = self.BWF_URL or self.INNOVATION_SUITE_URL
        self.BWF_USER = self.BWF_USER or self.INNOVATION_SUITE_USER
        self.BWF_PASSWORD = self.BWF_PASSWORD or self.INNOVATION_SUITE_PASSWORD

    LOG_LEVEL_CONSOLE: str = 'WARNING'
    LOG_LEVEL: str = 'DEBUG'
    LOG_FILE_SIZE: str = '10 MB'
    LOG_FILE_RETENTION: int = 5
    # same as loguru defaults, except !UTC makes the timestamp (as you might guess) in UTC not local time
    LOG_FILE_FORMAT: str = ('<green>{time:YYYY-MM-DD HH:mm:ss.SSS!UTC}</green> | '
                            '<level>{level: <8}</level> | '
                            '<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | '
                            '<level>{message}</level>')

    MAX_JOB_WORKERS: int | None = 4  # max number of worker threads executing jobs in the background

    # Root directory, under which the file system crawler searches.
    FS_DATA_SOURCE_DIR: str = "data"
    # Comma-separated list of case-sensitive glob patterns used by the file system crawler.
    FS_DATA_SOURCE_PATTERN: str = "**/*.pdf,**/*.PDF"

    # Prefix applied to every chunk prior to embeddings computation
    CHUNK_PREFIX: str = 'passage: '

    OPENSEARCH_HOST: str = 'clm-pun-vc2jwy.bmc.com'
    OPENSEARCH_PORT: int = 9200
    OPENSEARCH_USER: str = 'admin'
    OPENSEARCH_USER_PASSWORD: str = 'Hel!xGpt_1234'
    OPENSEARCH_SECURE: bool = True
    OPENSEARCH_VERIFY_CERTIFICATES: bool = False
    # Name of the index in which loaded documents are stored.
    # In prod, we would expect an index name specific to the tenant e.g., 'helixgpt-index-bmcprod'.
    OPENSEARCH_INDEX: str = 'helixgpt-index'

    # URL of the Innovation Suite server, which this application uses to store its queued jobs and which it pulls
    # some of its configurations from.
    # This is also used as the default URL for some of the integrations with BMC products (RKM, etc.).
    INNOVATION_SUITE_URL: str = 'http://clm-pun-vb3chm.bmc.com:8008'
    INNOVATION_SUITE_USER: str = 'rkhandel'
    INNOVATION_SUITE_PASSWORD: str = 'rkhandel'
    INNOVATION_SUITE_VERIFY_CERTIFICATES = False

    # Base URL of the RKM data source. This setting is most likely meant for development when we don't want or can
    # configure things in Innovation Suite. Defaults to `INNOVATION_SUITE_URL`.
    RKM_URL: str = None
    RKM_USER: str = None
    RKM_PASSWORD: str = None
    RKM_VERIFY_CERTIFICATES: bool = None

    # Base URL of the HKM. This setting is most likely meant for development when we don't want or can
    # configure things in Innovation Suite. Defaults to `INNOVATION_SUITE_URL`.
    HKM_URL: str = None
    HKM_USER: str = None
    HKM_PASSWORD: str = None

    BWF_URL: str = None
    BWF_USER: str = None
    BWF_PASSWORD: str = None

    # Amount of job steps a node will submit for execution at a time.
    JOB_STEP_BATCH_SIZE: int = 100

    class Config:
        env_file = ".env"  # relative to the uvicorn execution folder (not to the --app-dir option)
        env_file_encoding = 'utf-8'


Settings = Settings()
