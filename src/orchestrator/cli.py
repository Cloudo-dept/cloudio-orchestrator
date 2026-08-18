"""One installed script, two commands: `orchestrator api` and `orchestrator worker`."""

import asyncio
import logging
import signal

import typer
import uvicorn

from orchestrator.bootstrap import build
from orchestrator.config import Settings
from orchestrator.log import LOG_CONFIG_PATH, configure_logging, log_config_exists
from orchestrator.worker import OrchestratorWorker

app = typer.Typer(help="CloudIO orchestrator")
logger = logging.getLogger(__name__)


@app.command()
def api(host: str = "0.0.0.0", port: int = 8000, reload: bool = False) -> None:
    """Serve the HTTP API."""
    settings = Settings()
    configure_logging(settings.log_level)
    logger.info("Starting API server on %s:%s.", host, port)
    # Present -> hand uvicorn the path so its own startup and access records are ECS JSON from the
    # first line. Absent -> log_config=None, which makes uvicorn skip dictConfig entirely; the root
    # configuration configure_logging() just installed then stands, and uvicorn's loggers (no
    # handlers, propagate=True by default) fall through to it. The check has to happen here rather
    # than on a `uvicorn --log-config` command line: uvicorn open()s the path unguarded and dies
    # with FileNotFoundError when it is missing, so a raw command cannot express the absent branch.
    log_config = str(LOG_CONFIG_PATH) if log_config_exists() else None
    logger.info("uvicorn log config: %s.", log_config or "none (root config already installed)")
    uvicorn.run(
        "orchestrator.api:app",
        host=host,
        port=port,
        factory=False,
        reload=reload,
        log_config=log_config,
    )


@app.command()
def worker() -> None:
    """Run the daemon (claims due runs and drives them)."""

    async def _main() -> None:
        settings = Settings()
        # No uvicorn in this process — this call is the entire logging configuration.
        configure_logging(settings.log_level)
        logger.info("Booting orchestrator worker process.")
        container = await build(settings)

        # Wire graceful shutdown: SIGINT/SIGTERM ask the loops to drain and stop.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_shutdown, container.worker, sig)
            except NotImplementedError:  # e.g. Windows — fall back to default handling
                logger.warning("Signal handler for %s not supported on this platform.", sig.name)

        await container.worker.start()
        logger.info("Worker process exiting.")

    asyncio.run(_main())


def _request_shutdown(worker: OrchestratorWorker, sig: signal.Signals) -> None:
    logger.warning("Received %s; requesting graceful worker shutdown.", sig.name)
    worker.request_stop()
