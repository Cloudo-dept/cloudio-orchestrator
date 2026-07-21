*[← Index](README.md)*

# Incoming Endpoints — HTTP contract & request sources

Every HTTP endpoint the orchestrator API (`orchestrator.api`) exposes, its request/response
schema, and **who calls it**. Source is defined in
[`src/orchestrator/api.py`](../src/orchestrator/api.py); schemas referenced below live in
[`src/orchestrator/domain.py`](../src/orchestrator/domain.py).

All business endpoints are under `/api/v1`. Health probes are unversioned. There is **no auth on
the callbacks** — they are network-trust only (see
[01-external-contracts](01-external-contracts.md)).

## Endpoint summary

| Method & Path | Purpose | Request source |
| --- | --- | --- |
| `GET /healthz` | Liveness probe | Container platform (k8s liveness) |
| `GET /readyz` | Readiness probe (run store reachable) | Container platform (k8s readiness) |
| `POST /api/v1/workflows` | Register a workflow mapping | Platform admin / operator (CLI, admin UI) |
| `GET /api/v1/workflows` | List registered workflows | Platform admin / operator / console UI |
| `GET /api/v1/workflows/{identifier}` | Fetch one workflow | Platform admin / operator / console UI |
| `PUT /api/v1/workflows/{identifier}` | Update a workflow mapping | Platform admin / operator |
| `POST /api/v1/workflow-runs` | Trigger a run of a workflow | **Automation:** ServiceNow (RITM outbound REST). **Resource:** self-service portal / upstream automation |
| `GET /api/v1/workflow-runs/{run_id}` | Fetch one run's status/state | Requester / console UI polling for completion |
| `GET /api/v1/workflow-runs` | List/search runs (by ticket or resource) | Console UI / operator / integrating systems |
| `POST /api/v1/callbacks/ticket-approval` | Wake-early nudge on approval change | **ServiceNow** (business rule on approval change) |
| `POST /api/v1/callbacks/engine-run` | Wake-early nudge on engine completion | **Airflow** (DAG `on_success`/`on_failure_callback`) |

---

## Health

### `GET /healthz`
Liveness. Does not touch the database. Always `200`.

- **Source:** container orchestration platform (Kubernetes liveness probe / load balancer).
- **Request:** none.
- **Response `200` — `HealthResponse`:**

  | Field | Type | Notes |
  | --- | --- | --- |
  | `status` | `str` | Always `"ok"`. |

### `GET /readyz`
Readiness — pings the run store. `503` (not raised as an error page) when the store is
unreachable so the platform drains traffic instead of killing the pod.

- **Source:** container orchestration platform (Kubernetes readiness probe).
- **Request:** none.
- **Response `200` — `HealthResponse`:** `status = "ready"`.
- **Response `503` — `HealthResponse` detail:** `status` unavailable, `detail = "run store unreachable"`.

---

## Workflow registry

These manage the **registration** mapping a stable `identifier` to how a run is built and routed.
They are administrative — the callers are platform operators, not end users triggering work.

### `POST /api/v1/workflows`  → `201 Created`
Register a new workflow mapping.

- **Source:** platform admin / operator (via CLI or an admin console). *Not* an end user or an
  external provider.
- **Request — `WorkflowRegisterRequest`:**

  | Field | Type | Required | Notes |
  | --- | --- | --- | --- |
  | `identifier` | `str` (≤255) | ✔ | Stable key callers later trigger with. |
  | `run_type` | `RunType` enum | ✔ | `automation` \| `resource`. |
  | `engine_type` | `WorkflowEngineType` enum | ✔ | `airflow`. |
  | `automation_id` | `str` | ✔ | Airflow: the DAG id. |
  | `ticket_template_id` | `str` | ✔ | ServiceNow: catalog item sys_id. |
  | `name` | `str \| None` | ✗ | Human-friendly label. |

- **Response `201` — `WorkflowResponse`:** `workflow_id` (UUID), `identifier`, `name`, `run_type`,
  `engine_type`, `automation_id`, `ticket_template_id`.
- **Errors:** `409 Conflict` — identifier already registered.

### `GET /api/v1/workflows`  → `200`
List all registered workflows.

- **Source:** platform admin / operator / console UI.
- **Request:** none.
- **Response `200`:** `list[WorkflowResponse]`.

### `GET /api/v1/workflows/{identifier}`  → `200`
Fetch a single registered workflow.

- **Source:** platform admin / operator / console UI.
- **Path param:** `identifier: str`.
- **Response `200`:** `WorkflowResponse`.
- **Errors:** `404 Not Found` — unknown identifier.

### `PUT /api/v1/workflows/{identifier}`  → `200`
Update the editable fields of an existing registration (`identifier` is immutable, taken from the
path).

- **Source:** platform admin / operator.
- **Path param:** `identifier: str`.
- **Request — `WorkflowUpdateRequest`:** same fields as `WorkflowRegisterRequest` **minus**
  `identifier`: `run_type`, `engine_type`, `automation_id`, `ticket_template_id`, `name`.
- **Response `200`:** `WorkflowResponse`.
- **Errors:** `404 Not Found` — unknown identifier.

---

## Workflow runs

### `POST /api/v1/workflow-runs`  → `201 Created`
Trigger a run of a registered workflow. The caller names the workflow and supplies parameter sets;
the orchestrator resolves the registration to decide run type and build the run (`scheduled_at=now`
→ a worker claims it next scan).

- **Source depends on run type:**
  - **Automation** runs are triggered by **ServiceNow**: a user creates an RITM, and a ServiceNow
    outbound REST action calls this endpoint, passing the existing RITM in `ticket`. The run
    **attaches** to that RITM (it does *not* create one) and closes it when the automation finishes.
  - **Resource** runs are triggered by a self-service portal / upstream automation, and the
    orchestrator *creates* the RITM itself (catalog-item order + approval gate).
  - `created_by` identifies the human on whose behalf the run is created (becomes ServiceNow
    `caller_id` / `sysparm_requested_for` and the resource `last_modified_by`).
- **Request — `WorkflowRunTriggerRequest`:**

  | Field | Type | Required | Notes |
  | --- | --- | --- | --- |
  | `workflow_identifier` | `str` | ✔ | Names a registered workflow. |
  | `created_by` | `str` (≤255) | ✔ | Requesting user. |
  | `max_retries` | `int` (0–10) | ✗ (default `3`) | Per-step transient-retry budget. `0` = escalate immediately. |
  | `ticket_params` | `dict[str, Any]` | ✗ (`{}`) | Provider ticket template variables (pass-through; used only when the orchestrator creates the RITM, i.e. resource runs). |
  | `workflow_params` | `dict[str, Any]` | ✗ (`{}`) | Engine conf (pass-through). |
  | `resource` | `ResourceSpec \| None` | ✗ | **Required for `resource` run types**; omit for `automation`. |
  | `ticket` | `TicketRef \| None` | ✗ | The pre-existing RITM to attach to. **Required for `automation` run types**; ignored for `resource` runs. `TicketRef` = `{ ticket_id, native_id }` (RITM number + sys_id). |

  `ResourceSpec`: `project_id`, `resource_type`, `operation` (`create`\|`update`\|`delete`, default
  `create`), `vendor_id` (blank for CREATE — assigned from run id; set only to target an existing
  record on UPDATE/DELETE), `name`, `region?`, `environment?`, `description`, `tags[]`, `data{}`,
  `alert_groups[]`.

- **Response `201` — `WorkflowRunResponse`:** `run_id` (UUID), `run_type`, `status` (`RunStatus`),
  `current_step` (`str \| None`), `created_by`, `max_retries`, `run_state` (typed `RunState`).
- **Errors:** `404` — unknown workflow; `422` — `resource` missing for a resource workflow, or
  `ticket` missing for an automation workflow.

### `GET /api/v1/workflow-runs/{run_id}`  → `200`
Fetch a single run's current status and state. This is the **polling** endpoint requesters use to
observe completion (completion is poll-based; callbacks only cut latency).

- **Source:** the requester / console UI polling for the run's outcome.
- **Path param:** `run_id: uuid.UUID`.
- **Response `200`:** `WorkflowRunResponse`.
- **Errors:** `404 Not Found` — unknown run.

### `GET /api/v1/workflow-runs`  → `200`
List recent runs, or search by ticket / resource.

- **Source:** console UI / operator / integrating systems.
- **Query params (optional):**
  - `ticket_id: str` — return runs tied to that ticket.
  - `resource_id: str` — return runs tied to that resource (`vendor_id`).
  - Neither → most recent runs, newest first.
- **Response `200`:** `list[WorkflowRunResponse]`.

---

## Callbacks (wake-early optimization)

Both callbacks **only nudge** a waiting run to re-poll now (`runs.wake(...)`, `scheduled_at=now`).
They carry a *neutral reference only* — never a provider status — and never write run state; the
re-driven poll step reads the authoritative external status, so polling remains the source of
truth. **Idempotent:** an unknown or already-terminal reference is a no-op (`woken=0`), never an
error. **Auth: network-trust** (no HMAC/token). See [01-external-contracts](01-external-contracts.md).

### `POST /api/v1/callbacks/ticket-approval`  → `202 Accepted`
- **Source:** **ServiceNow** — a business rule POSTing on an approval-state change (configured in
  ServiceNow).
- **Request — `TicketApprovalCallbackRequest`:**

  | Field | Type | Notes |
  | --- | --- | --- |
  | `ticket_id` | `str` | Neutral ticket id (e.g. RITM number). No status carried. |

- **Response `202` — `CallbackAcceptedResponse`:** `woken: int` — how many waiting runs were made
  due now (`0` = nothing matched / already terminal).

### `POST /api/v1/callbacks/engine-run`  → `202 Accepted`
- **Source:** **Airflow** — the DAG's best-effort `on_success_callback` / `on_failure_callback`
  POSTs the DAG run id (see [`airflow/dags/provision_vm.py`](../airflow/dags/provision_vm.py)).
  Enabled by setting `ORCHESTRATOR_BASE_URL` in the Airflow environment.
- **Request — `EngineRunCallbackRequest`:**

  | Field | Type | Notes |
  | --- | --- | --- |
  | `engine_run_id` | `str` | Neutral engine run id (the string `trigger_workflow` returned). No status carried. |

- **Response `202` — `CallbackAcceptedResponse`:** `woken: int`.

---

## Enum reference

- **`RunType`:** `automation`, `resource`.
- **`RunStatus`:** `pending`, `running`, `completed`, `failed`, `rejected`.
- **`WorkflowEngineType`:** `airflow`.
- **`ResourceOperation`:** `create`, `update`, `delete`.
