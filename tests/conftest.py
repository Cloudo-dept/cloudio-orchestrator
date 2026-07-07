"""Shared fixtures: in-memory port fakes for the unit/orchestration tests."""

import pytest

from orchestrator.config import Settings
from tests.fakes import (
    FakeResourceManagerClient,
    FakeTicketSystemClient,
    FakeWorkflowEngineClient,
    FakeWorkflowRepository,
    FakeWorkflowRunRepository,
)


@pytest.fixture
def runs() -> FakeWorkflowRunRepository:
    return FakeWorkflowRunRepository()


@pytest.fixture
def workflows() -> FakeWorkflowRepository:
    return FakeWorkflowRepository()


@pytest.fixture
def tickets() -> FakeTicketSystemClient:
    return FakeTicketSystemClient()


@pytest.fixture
def resources() -> FakeResourceManagerClient:
    return FakeResourceManagerClient()


@pytest.fixture
def engine() -> FakeWorkflowEngineClient:
    return FakeWorkflowEngineClient()


@pytest.fixture
def settings() -> Settings:
    # model_construct skips validation so we don't need the full required env just to read
    # retry_base_seconds; other fields keep their declared defaults.
    return Settings.model_construct(retry_base_seconds=10.0)
