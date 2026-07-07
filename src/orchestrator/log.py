"""Loguru configuration.

One ``configure_logging`` call per process entrypoint (the ``worker`` and ``api`` commands).
Business logic never configures logging — it just does ``from loguru import logger`` and logs;
the sink installed here decides format and threshold. ``diagnose`` is kept off so tracebacks
never render local variables (which can hold secrets/tokens) into the logs.
"""

import sys

from loguru import logger

_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)


def configure_logging(level: str = "INFO") -> None:
    """Install a single stderr sink at ``level``, replacing loguru's default handler.

    Idempotent enough for entrypoints: it removes any existing handlers first, so calling it
    twice (e.g. tests) does not double every line. ``enqueue=True`` makes logging safe across
    the worker's concurrent loops and non-blocking on the hot path.
    """
    logger.remove()
    logger.add(
        sys.stderr,
        level=level.upper(),
        format=_LOG_FORMAT,
        backtrace=True,
        diagnose=False,
        enqueue=True,
    )
    logger.info("Logging configured; stderr sink at level {}.", level.upper())
