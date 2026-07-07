"""Application settings (pydantic-settings).

One database DSN — the run store and the workflow registry live in the same Postgres. There is no
separate queue, so there is no second DSN to derive.
"""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ORCH_", env_file=".env", extra="ignore")

    # Single Postgres DSN (SQLAlchemy async form), e.g. postgresql+asyncpg://user:pw@host/db.
    # The run store and the workflow registry both live in this database.
    database_url: str

    # Logging (loguru): threshold for the stderr sink. TRACE/DEBUG/INFO/WARNING/ERROR/CRITICAL.
    log_level: str = "INFO"

    # Retry policy (application-owned; expressed as WorkflowRun.scheduled_at, never a queue delay).
    retry_base_seconds: float = 10.0  # base for per-step exponential backoff

    # Workers (each loop claims one due run and drives it; the pool size sets concurrency)
    worker_concurrency_limit: int = 16  # RunWorker loops per daemon
    worker_poll_interval_seconds: float = 1.0  # idle re-check interval per worker
    redrive_lease_seconds: float = 300.0  # re-drive lease: a claimed run reappears if its
    # processing never completes (crash recovery).

    # Airflow (first workflow engine) — REST API v2, token auth, verify=False per spec
    airflow_base_url: str
    airflow_username: str
    airflow_password: SecretStr

    # ServiceNow (first ticket system)
    servicenow_base_url: str
    servicenow_username: str
    servicenow_password: SecretStr
    # team-name -> full ServiceNow assignment-group name; unknown names are used verbatim.
    servicenow_responsible_groups: dict[str, str] = Field(default_factory=dict)
    servicenow_incident_team: str = "cloudio"  # default team for failure incidents

    # Project Manager (first resource manager)
    pm_base_url: str
    pm_token: SecretStr

    external_call_timeout_seconds: float = 10.0
