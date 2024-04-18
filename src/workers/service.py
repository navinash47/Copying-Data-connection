from concurrent.futures import ThreadPoolExecutor
from loguru import logger

from config import Settings


class WorkerGroup:
    """
    A thread group that will concurrently execute the specified `do_work`
    function on calls to `__worker()`.

    The max number of worker threads of the underlying ThreadPoolExecutor is driven by `Settings.MAX_JOB_WORKERS`.
    """

    def __init__(self, do_work):
        """
        :type do_work: function called to perform the work
        """
        self.do_work = do_work
        self.executor = ThreadPoolExecutor(Settings.MAX_JOB_WORKERS, thread_name_prefix='job_worker')

    def submit_work(self, work):
        self.executor.submit(self.__worker, work)

    def __worker(self, work):
        try:
            self.do_work(work)
        except Exception:
            logger.exception('unhandled exception in __worker()')

    def shutdown(self):
        """ Shuts down the thread executor. Provided for tests for now. """
        self.executor.shutdown()
