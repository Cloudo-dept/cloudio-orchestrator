"""Legacy automation runner adapter.

The runner takes a request and says nothing more about it, so this adapter has exactly one call.
Everything the runner's wire format needs is already spelled in ``LegacyRequest`` — see the note
there on why that one model carries a provider's field names — so the body is the request as it
stands, with the absent optional fields left out rather than sent as nulls.
"""

import logging
from typing import Any

import httpx

from orchestrator.domain import LegacyRequest
from orchestrator.ports import LegacyAutomationClient

logger = logging.getLogger(__name__)


class HttpLegacyAutomationClient(LegacyAutomationClient):
    """Posts the request to the legacy runner's submit endpoint.

    ``submit_path`` is configuration rather than a constant: the runner's path is deployment
    knowledge, and hard-coding a guess here would put a wrong URL somewhere that looks
    authoritative. See ``Settings.legacy_runner_submit_path``.
    """

    # The keys a reference can arrive under. The runner's acknowledgement is its own shape, so the
    # reference is best-effort: it is recorded for an operator chasing a request, and a response
    # that carries nothing recognisable is not a failure to hand the request over.
    _REFERENCE_KEYS = ("request_id", "id", "reference", "run_id")

    def __init__(self, client: httpx.AsyncClient, submit_path: str) -> None:
        # One pooled client for the process, built and closed by the composition root.
        self._http = client
        self._submit_path = submit_path

    async def submit(self, request: LegacyRequest, idempotency_key: str) -> str | None:
        # exclude_none keeps an unset project_id/resource_type out of the body entirely: both are
        # optional to the runner, and a null is a value, not a silence.
        body = request.model_dump(mode="json", exclude_none=True)
        logger.info(
            "Submitting legacy request (flow_type=%s, db_operation=%s, name=%s) to the legacy "
            "automation runner.",
            request.flow_type,
            request.db_operation,
            request.name,
        )
        resp = await self._http.post(
            self._submit_path,
            json=body,
            headers={"Idempotency-Key": idempotency_key},  # orchestrator-added
        )
        resp.raise_for_status()
        reference = self._reference(resp)
        logger.info(
            "Legacy runner accepted the %s request for '%s'%s.",
            request.flow_type,
            request.name,
            f" as {reference}" if reference else "",
        )
        return reference

    def _reference(self, resp: httpx.Response) -> str | None:
        """The runner's own id for the accepted request, where the acknowledgement carries one.
        Never raises: losing the reference must not turn a successful handover into a failure that
        gets retried into a second one."""
        try:
            body: Any = resp.json()
        except ValueError:
            return None
        if not isinstance(body, dict):
            return None
        for key in self._REFERENCE_KEYS:
            value = body.get(key)
            if isinstance(value, str | int) and str(value):
                return str(value)
        return None
