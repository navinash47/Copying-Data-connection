from enum import IntEnum
from types import SimpleNamespace


class JobType(IntEnum):
    CRAWL = 0
    LOAD = 1
    SYNC_DELETIONS = 2
    DELETE = 3


class JobStepStatus(IntEnum):
    PENDING = 0
    PARKED = 1000
    IN_PROGRESS = 2000
    DONE = 3000
    ERROR = 4000


Datasource = SimpleNamespace()
Datasource.RKM = 'RKM'
Datasource.HKM = 'HKM'
Datasource.BWF = 'BWF'
Datasource.SHAREPOINT = 'SPT'
Datasource.CONFLUENCE = 'CNF'
