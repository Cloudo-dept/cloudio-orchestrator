"""Real HttpLegacyAutomationClient driven against the LegacyRunnerMock."""

import httpx
import pytest

from orchestrator.adapters.legacy_automation import HttpLegacyAutomationClient
from tests.factories import make_legacy_request
from tests.mocks.base import Override, mock_client
from tests.mocks.legacy_runner import LegacyRunnerMock


async def test_submit_posts_the_request_verbatim(
    legacy_runner: LegacyRunnerMock, legacy_client: HttpLegacyAutomationClient
) -> None:
    reference = await legacy_client.submit(make_legacy_request(), "run-1:submitting_legacy")

    assert reference == "legacy-1"
    assert legacy_runner.keys == ["run-1:submitting_legacy"]
    # The runner's own field names, untranslated — no orchestrator vocabulary on the wire.
    assert legacy_runner.submissions == [
        {
            "flow_type": "legacy-provision-vm",
            "project_id": "proj-1",
            "resource_type": "vm",
            "variables": {"size": "large"},
            "db_operation": "create",
            "name": "app-01",
            "region": "gvt",
        }
    ]


async def test_submit_omits_absent_optional_fields(
    legacy_runner: LegacyRunnerMock, legacy_client: HttpLegacyAutomationClient
) -> None:
    # Both are optional to the runner, and a null is a value, not a silence.
    request = make_legacy_request(project_id=None, resource_type=None)

    await legacy_client.submit(request, "run-1:submitting_legacy")

    body = legacy_runner.submissions[0]
    assert "project_id" not in body and "resource_type" not in body
    assert body["flow_type"] == "legacy-provision-vm"


async def test_submit_replay_under_the_same_key_is_not_a_second_request(
    legacy_runner: LegacyRunnerMock, legacy_client: HttpLegacyAutomationClient
) -> None:
    # Only holds for a runner that honours the header — see SubmitLegacyStep on the residual risk.
    a = await legacy_client.submit(make_legacy_request(), "same-key")
    b = await legacy_client.submit(make_legacy_request(), "same-key")

    assert a == b == "legacy-1"
    assert len(legacy_runner.submissions) == 1


@pytest.mark.parametrize("key", ["request_id", "id", "reference", "run_id"])
async def test_a_reference_is_read_from_any_of_the_known_keys(key: str) -> None:
    runner = LegacyRunnerMock(ack_reference_key=key)
    async with mock_client(runner.app, "http://legacy.local") as http:
        client = HttpLegacyAutomationClient(http, runner.submit_path)
        assert await client.submit(make_legacy_request(), "run-1:submitting_legacy") == "legacy-1"


async def test_an_unrecognised_acknowledgement_still_counts_as_accepted() -> None:
    # Losing the reference must not turn a successful handover into a failure that gets retried
    # into a second one.
    runner = LegacyRunnerMock(ack_reference_key=None)
    async with mock_client(runner.app, "http://legacy.local") as http:
        client = HttpLegacyAutomationClient(http, runner.submit_path)
        assert await client.submit(make_legacy_request(), "run-1:submitting_legacy") is None
    assert len(runner.submissions) == 1


async def test_a_refused_handover_raises_for_the_caller_to_retry(
    legacy_runner: LegacyRunnerMock, legacy_client: HttpLegacyAutomationClient
) -> None:
    legacy_runner.overrides.append(Override(path_contains="/requests", status=500))

    with pytest.raises(httpx.HTTPStatusError):
        await legacy_client.submit(make_legacy_request(), "run-1:submitting_legacy")


async def test_the_submit_path_comes_from_configuration() -> None:
    runner = LegacyRunnerMock(submit_path="/legacy/v2/submit")
    async with mock_client(runner.app, "http://legacy.local") as http:
        client = HttpLegacyAutomationClient(http, "/legacy/v2/submit")
        await client.submit(make_legacy_request(), "run-1:submitting_legacy")
    assert runner.requests == [("POST", "/legacy/v2/submit")]
