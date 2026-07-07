"""Settings load from ORCH_-prefixed env vars, with typed defaults and secret handling."""

import pytest

from orchestrator.config import Settings

REQUIRED_ENV = {
    "ORCH_DATABASE_URL": "postgresql+asyncpg://u:pw@localhost/db",
    "ORCH_AIRFLOW_BASE_URL": "https://airflow.example",
    "ORCH_AIRFLOW_USERNAME": "airflow",
    "ORCH_AIRFLOW_PASSWORD": "af-secret",
    "ORCH_SERVICENOW_BASE_URL": "https://sn.example",
    "ORCH_SERVICENOW_USERNAME": "snuser",
    "ORCH_SERVICENOW_PASSWORD": "sn-secret",
    "ORCH_PM_BASE_URL": "https://pm.example",
    "ORCH_PM_TOKEN": "pm-token",
}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    # Ensure a stray .env in the cwd doesn't interfere with defaults under test.
    monkeypatch.chdir("/tmp")


def test_settings_load_from_env_with_defaults(env: None) -> None:
    settings = Settings()
    assert settings.database_url == "postgresql+asyncpg://u:pw@localhost/db"
    # Defaults for the worker knobs.
    assert settings.worker_concurrency_limit == 16
    assert settings.worker_poll_interval_seconds == 1.0
    assert settings.redrive_lease_seconds == 300.0
    assert settings.retry_base_seconds == 10.0
    assert settings.servicenow_incident_team == "cloudio"
    assert settings.servicenow_responsible_groups == {}


def test_secrets_are_wrapped(env: None) -> None:
    settings = Settings()
    # SecretStr masks in repr but exposes the value explicitly.
    assert "sn-secret" not in repr(settings)
    assert settings.servicenow_password.get_secret_value() == "sn-secret"
    assert settings.pm_token.get_secret_value() == "pm-token"


def test_responsible_groups_parsed_from_json(env: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ORCH_SERVICENOW_RESPONSIBLE_GROUPS", '{"netops": "CloudIO NetOps"}')
    monkeypatch.setenv("ORCH_WORKER_CONCURRENCY_LIMIT", "4")
    settings = Settings()
    assert settings.servicenow_responsible_groups == {"netops": "CloudIO NetOps"}
    assert settings.worker_concurrency_limit == 4


def test_no_queue_dsn_attribute(env: None) -> None:
    # The queue was cut; there is no derived DSN anymore.
    settings = Settings()
    assert not hasattr(settings, "queue_dsn")
