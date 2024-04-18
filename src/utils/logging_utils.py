import inspect
import logging
import sys

from loguru import logger
from config import Settings


class InterceptHandler(logging.Handler):
    def emit(self, record):
        # Get corresponding Loguru level if it exists.
        level: str | int
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # Find caller from where originated the logged message.
        frame, depth = inspect.currentframe(), 0
        while frame and (depth == 0 or frame.f_code.co_filename == logging.__file__):
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def setup_logging():
    # intercept everything at the root logger
    logging.root.handlers = [InterceptHandler()]
    logging.root.setLevel(Settings.LOG_LEVEL)

    # remove every other logger's handlers
    # and propagate to root logger
    for name in logging.root.manager.loggerDict.keys():
        logging.getLogger(name).handlers = []
        logging.getLogger(name).propagate = True

    # configure loguru
    logger.configure(handlers=[
        {
            "sink": sys.stdout,
            "serialize": False,
            "level": Settings.LOG_LEVEL_CONSOLE,
            "format": Settings.LOG_FILE_FORMAT,
        },
        {
            "sink": "./logs/server.log",
            "serialize": False,
            "level": Settings.LOG_LEVEL,
            "format": Settings.LOG_FILE_FORMAT,
            "rotation": Settings.LOG_FILE_SIZE,
            "retention": Settings.LOG_FILE_RETENTION,
            "compression": "gz",
            "colorize": False
        }
    ])
