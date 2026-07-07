"""Real ProjectManagerResourceClient driven against the stateful ProjectManagerMock."""

import httpx
import pytest

from orchestrator.adapters.project_manager import ProjectManagerResourceClient
from tests.mocks.base import Override
from tests.mocks.project_manager import ProjectManagerMock


async def test_create_resource_posts_body_and_key(
    project_manager: ProjectManagerMock, pm_client: ProjectManagerResourceClient
) -> None:
    result = await pm_client.create_resource(
        "proj-1", "vm", {"vendor_id": "vm-1", "name": "app"}, "run-1:configuring_resource:0"
    )
    assert result["vendor_id"] == "vm-1" and result["in_progress"] is True
    assert "proj-1/vm/vm-1" in project_manager.resources


async def test_create_resource_replay_is_idempotent(
    project_manager: ProjectManagerMock, pm_client: ProjectManagerResourceClient
) -> None:
    a = await pm_client.create_resource("proj-1", "vm", {"vendor_id": "vm-1"}, "same-key")
    b = await pm_client.create_resource("proj-1", "vm", {"vendor_id": "vm-1"}, "same-key")
    assert a == b and len(project_manager.resources) == 1


async def test_finalize_patches_in_progress_false(
    project_manager: ProjectManagerMock, pm_client: ProjectManagerResourceClient
) -> None:
    await pm_client.create_resource("proj-1", "vm", {"vendor_id": "vm-1"}, "run-1:creating:0")
    await pm_client.update_resource("proj-1", "vm", "vm-1", {"in_progress": False})
    assert project_manager.patches[-1] == {"vendor_id": "vm-1", "in_progress": False}
    assert project_manager.resources["proj-1/vm/vm-1"]["in_progress"] is False


async def test_create_5xx_is_surfaced(
    project_manager: ProjectManagerMock, pm_client: ProjectManagerResourceClient
) -> None:
    project_manager.overrides.append(Override(path_contains="/project_resources", status=503))
    with pytest.raises(httpx.HTTPStatusError):
        await pm_client.create_resource("proj-1", "vm", {"vendor_id": "vm-1"}, "k")
