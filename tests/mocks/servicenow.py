"""Stateful ServiceNow mock: faithful RITM/incident semantics + seeding & override knobs."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI, Request, Response

from tests.mocks.base import Override, apply_overrides


@dataclass
class Ritm:
    number: str
    sys_id: str
    request_sys_id: str
    correlation_id: str | None = None
    state: int | None = None
    approval: str = "requested"  # ServiceNow RITM approval field: requested/approved/rejected
    work_notes: list[str] = field(default_factory=list)
    requested_for: str | None = None  # sysparm_requested_for the order was placed with
    assignment_group: str | None = None  # sys_user_group sys_id the RITM was assigned to


@dataclass
class Incident:
    number: str
    sys_id: str
    body: dict[str, Any]


@dataclass
class ServiceNowMock:
    ritms: list[Ritm] = field(default_factory=list)
    incidents: list[Incident] = field(default_factory=list)
    overrides: list[Override] = field(default_factory=list)
    requests: list[tuple[str, str]] = field(default_factory=list)
    default_approval: str = "approved"  # approval state ordered RITMs come back with
    users: dict[str, str] = field(default_factory=lambda: {"jdoe": "usersys0000001"})
    # sys_user_group rows: group name -> sys_id. What the adapter resolves an unmapped name against;
    # clear it to model a name that exists nowhere. "cloudio" is the default incident team.
    groups: dict[str, str] = field(
        default_factory=lambda: {"CloudIO NetOps": "grpsys0000002", "cloudio": "grpsys0000003"}
    )
    _seq: int = 0

    def _mint(self, prefix: str) -> tuple[str, str]:
        self._seq += 1
        return f"{prefix}{self._seq:07d}", f"{prefix.lower()}sys{self._seq:07d}"

    # scenario knob: pretend a RITM already exists for this idempotency key
    def seed_ritm(self, correlation_id: str) -> Ritm:
        number, sys_id = self._mint("RITM")
        r = Ritm(
            number=number, sys_id=sys_id, request_sys_id="req-seed", correlation_id=correlation_id
        )
        self.ritms.append(r)
        return r

    @property
    def app(self) -> FastAPI:
        return _build(self)


def _build(mock: ServiceNowMock) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def _record_override(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        mock.requests.append((request.method, request.url.path))
        forced = apply_overrides(mock.overrides, request)
        return forced if forced is not None else await call_next(request)

    @app.get("/api/now/table/sc_req_item")
    async def query_ritm(sysparm_query: str = "") -> dict[str, Any]:
        # correlation_id=<key> (idempotency lookup) or request=<sys_id> (post-order resolve)
        field_name, _, value = sysparm_query.partition("=")
        if field_name == "correlation_id":
            hits = [r for r in mock.ritms if r.correlation_id == value]
        elif field_name == "request":
            hits = [r for r in mock.ritms if r.request_sys_id == value]
        else:
            hits = []
        return {"result": [{"number": r.number, "sys_id": r.sys_id} for r in hits[:1]]}

    @app.get("/api/now/table/sys_user")
    async def query_user(sysparm_query: str = "") -> dict[str, Any]:
        # user_param=<login>; unknown logins return no rows, like the real table
        field_name, _, value = sysparm_query.partition("=")
        sys_id = mock.users.get(value) if field_name == "user_param" else None
        return {"result": [{"sys_id": sys_id}] if sys_id else []}

    @app.get("/api/now/table/sys_user_group")
    async def query_group(sysparm_query: str = "") -> dict[str, Any]:
        # name=<group name>; a group nobody created returns no rows
        field_name, _, value = sysparm_query.partition("=")
        sys_id = mock.groups.get(value) if field_name == "name" else None
        return {"result": [{"sys_id": sys_id}] if sys_id else []}

    @app.get("/api/now/table/sc_req_item/{sys_id}")
    async def get_ritm(sys_id: str) -> dict[str, Any]:
        r = next(r for r in mock.ritms if r.sys_id == sys_id)
        return {"result": {"number": r.number, "sys_id": r.sys_id, "approval": r.approval}}

    @app.post("/api/sn_sc/servicecatalog/items/{catalog_sys_id}/order_now")
    async def order(catalog_sys_id: str, body: dict[str, Any]) -> dict[str, Any]:
        number, sys_id = mock._mint("RITM")
        _, request_sys_id = mock._mint("REQ")
        mock.ritms.append(
            Ritm(
                number=number,
                sys_id=sys_id,
                request_sys_id=request_sys_id,
                approval=mock.default_approval,
                requested_for=body.get("sysparm_requested_for"),
            )
        )
        return {"result": {"sys_id": request_sys_id}}

    @app.patch("/api/now/table/sc_req_item/{sys_id}")
    async def patch_ritm(sys_id: str, body: dict[str, Any]) -> dict[str, Any]:
        r = next(r for r in mock.ritms if r.sys_id == sys_id)
        if "correlation_id" in body:
            r.correlation_id = body["correlation_id"]
        if "state" in body:
            r.state = body["state"]
        if "work_notes" in body:
            r.work_notes.append(body["work_notes"])
        if "assignment_group" in body:
            r.assignment_group = body["assignment_group"]
        return {"result": {"number": r.number, "sys_id": r.sys_id}}

    @app.post("/api/now/table/incident")
    async def open_incident(body: dict[str, Any]) -> dict[str, Any]:
        number, sys_id = mock._mint("INC")
        mock.incidents.append(Incident(number=number, sys_id=sys_id, body=body))
        return {"result": {"number": number, "sys_id": sys_id}}

    return app
