"""One installed script, two commands: `orchestrator api` and `orchestrator worker`."""

import asyncio
import signal

import typer
import uvicorn
from loguru import logger

from orchestrator.bootstrap import build
from orchestrator.config import Settings
from orchestrator.log import configure_logging
from orchestrator.worker import OrchestratorWorker

app = typer.Typer(help="CloudIO orchestrator")


@app.command()
def api(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Serve the HTTP API."""
    settings = Settings()
    configure_logging(settings.log_level)
    logger.info("Starting API server on {}:{}.", host, port)
    uvicorn.run("orchestrator.api:app", host=host, port=port, factory=False)


@app.command()
def worker() -> None:
    """Run the daemon (claims due runs and drives them)."""

    async def _main() -> None:
        settings = Settings()
        configure_logging(settings.log_level)
        logger.info("Booting orchestrator worker process.")
        container = await build(settings)

        # Wire graceful shutdown: SIGINT/SIGTERM ask the loops to drain and stop.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _request_shutdown, container.worker, sig)
            except NotImplementedError:  # e.g. Windows — fall back to default handling
                logger.warning("Signal handler for {} not supported on this platform.", sig.name)

        await container.worker.start()
        logger.info("Worker process exiting.")

    asyncio.run(_main())


def _request_shutdown(worker: OrchestratorWorker, sig: signal.Signals) -> None:
    logger.warning("Received {}; requesting graceful worker shutdown.", sig.name)
    worker.request_stop()
