import logging
import os
from functools import lru_cache

import structlog


DEFAULT_INFO_LOGGER_PROCESSORS = [
    structlog.stdlib.filter_by_level,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S")
]

DEFAULT_ERROR_LOGGER_PROCESSORS = [
    structlog.stdlib.filter_by_level,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.stdlib.PositionalArgumentsFormatter(),
    structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M.%S"),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info
]


class LevelLogger(structlog.PrintLogger):
    def __init__(self, name, level, *args, **kwargs):
        self.name = name
        self.level = level
        super().__init__(*args, **kwargs)

    @lru_cache()
    def isEnabledFor(self, level):
        return self.level <= level


_cached_loggers = []


class BoundLogger(structlog.stdlib.BoundLogger):

    _orig_context = {}

    def __init__(self, logger, processors, context):
        super().__init__(logger, processors, context)
        if not BoundLogger._orig_context:
            BoundLogger._orig_context = context.copy()

    def renew(self, **keys):
        return self.new(**keys, **self._orig_context)


def setup_logging(info_logger_processors: list = [],
                  error_logger_processors: list = [],
                  default_logging_level: int = logging.INFO):

    if _cached_loggers:
        return _cached_loggers

    APP_MODE = os.environ.get('APP_MODE', 'test')
    istest = APP_MODE == 'test'

    if default_logging_level is None:
        default_logging_level = logging.DEBUG if istest else logging.INFO

    info_processors = info_logger_processors or DEFAULT_INFO_LOGGER_PROCESSORS[:]
    error_processors = error_logger_processors or DEFAULT_ERROR_LOGGER_PROCESSORS[:]
    info_logger_wrapper_class = BoundLogger
    error_logger_wrapper_class = BoundLogger

    added_processor = APP_MODE in ['dev', 'test'] and structlog.dev.ConsoleRenderer() or structlog.processors.JSONRenderer()

    if not info_logger_processors:
        info_processors.append(added_processor)
    if not error_logger_processors:
        error_processors.append(added_processor)

    structlog.configure(
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
        cache_logger_on_first_use=True
    )

    infologger = LevelLogger('postschema.log', default_logging_level)
    accesslogger = LevelLogger('postschema.access', default_logging_level)
    errorlogger = LevelLogger('postschema.error', logging.ERROR)

    info_logger = structlog.wrap_logger(
        infologger,
        processors=info_processors,
        wrapper_class=info_logger_wrapper_class)

    error_logger = structlog.wrap_logger(
        errorlogger,
        processors=error_processors,
        wrapper_class=error_logger_wrapper_class
    )

    access_logger = structlog.wrap_logger(
        accesslogger,
        processors=info_processors,
        wrapper_class=info_logger_wrapper_class
    )

    if not _cached_loggers:
        _cached_loggers.extend([info_logger, error_logger, access_logger])

    return info_logger, error_logger, access_logger
