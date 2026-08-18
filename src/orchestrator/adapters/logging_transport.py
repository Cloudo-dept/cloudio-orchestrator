"""An httpx transport that logs failed outbound calls.

Every provider adapter already accepts a ``transport`` as a documented test seam and passes it
straight to ``httpx.AsyncClient``, but nothing was ever injected in production. Wrapping that seam
instruments **every** outbound call in the system from one place: no adapter method changes, no
port changes, and nothing added to the ~14 call sites that would each have needed their own log
line (and would still have missed the N+1 XCom loop inside ``AirflowWorkflowEngineClient``).

Failures only, deliberately. ``query_run_status`` and ``get_approval_status`` fire on every poll of
every waiting run, so a record per call would scale with (waiting runs x poll frequency) and say
almost nothing. Latency and call rates belong in Prometheus; what a log can add is *which* call
failed, for *which* run — the ContextVar in ``log.py`` supplies the latter automatically.

Nothing here is provider-specific: it sees only HTTP. The provider name is passed in by the
composition root, which is the one place that knows which adapter is being wired.
"""

import logging

import httpx

logger = logging.getLogger(__name__)


class FailureLoggingTransport(httpx.AsyncBaseTransport):
    """Delegates to a real transport, logging non-2xx responses and connection failures.

    Only the request line and status are logged — never headers (they carry Basic auth and bearer
    tokens) and never bodies (ServiceNow echoes caller-supplied ticket_params, the most likely
    place for PII in this system).
    """

    def __init__(self, provider: str, transport: httpx.AsyncBaseTransport) -> None:
        self.provider = provider
        self.transport = transport

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        try:
            response = await self.transport.handle_async_request(request)
        except Exception:
            # Connect/read timeouts and DNS failures never reach a status code. Without this they
            # surface far downstream as a generic "step raised" with no indication of which
            # provider was unreachable.
            logger.exception(
                "%s call failed: %s %s did not complete.",
                self.provider,
                request.method,
                request.url.path,
            )
            raise

        if response.status_code >= 400:
            # Not an error log: the caller decides whether this is fatal. A 409 is how Airflow
            # reports a duplicate dag_run_id and how ServiceNow signals an existing record — both
            # are normal idempotent outcomes, so warning is the honest level.
            logger.warning(
                "%s call returned %s: %s %s.",
                self.provider,
                response.status_code,
                request.method,
                request.url.path,
            )
        return response

    async def aclose(self) -> None:
        await self.transport.aclose()
