"""Dev-only Project Manager mock.

A FastAPI + MongoDB stand-in for the real Project Manager API. It implements the resource
endpoints the orchestrator's ``ProjectManagerResourceClient`` calls, plus convenience endpoints
(list, and project creation) for seeding and eyeballing state during development. State persists
in a dedicated MongoDB database so it survives restarts.

NOT for production — no auth is enforced and indexes are created on startup (no migrations).
"""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

connection_string = os.environ.get("PM_DATABASE_URL", "mongodb://localhost:27017/")

client: AsyncIOMotorClient[dict[str, Any]] = AsyncIOMotorClient(connection_string)
db: AsyncIOMotorDatabase[dict[str, Any]] = client["pm"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # dev convenience: no migrations — mirror the previous schema's unique constraints as indexes.
    await db["projects"].create_index("project_id", unique=True)
    await db["resources"].create_index("idempotency_key", unique=True)
    await db["resources"].create_index(["project_id", "resource_type", "vendor_id"])
    yield


app = FastAPI(title="Project Manager (dev mock)", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/projects")
async def create_project(body: dict[str, Any]) -> dict[str, Any]:
    """Create a project. Idempotent on the caller-supplied ``project_id`` (a repeat returns the
    existing project rather than erroring)."""
    project_id = body.get("project_id")
    if not project_id:
        raise HTTPException(422, detail="project_id is required")
    existing = await db["projects"].find_one({"project_id": project_id})
    if existing is not None:
        existing_body: dict[str, Any] = existing["body"]
        return existing_body

    project = {**body, "name": body.get("name", project_id)}
    await db["projects"].insert_one(
        {"project_id": project_id, "name": project["name"], "body": project}
    )
    return project


@app.get("/projects")
async def list_projects() -> list[dict[str, Any]]:
    """Convenience for eyeballing state during development."""
    rows = await db["projects"].find().to_list(length=None)
    return [r["body"] for r in rows]


@app.post("/projects/{project_id}/project_resources/{resource_type}")
async def create_resource(
    project_id: str,
    resource_type: str,
    body: dict[str, Any],
    idempotency_key: str = Header(alias="Idempotency-Key"),
) -> dict[str, Any]:
    # Idempotent on the orchestrator-supplied key, and on the natural key (vendor_id).
    by_key = await db["resources"].find_one({"idempotency_key": idempotency_key})
    if by_key is not None:
        by_key_body: dict[str, Any] = by_key["body"]
        return by_key_body

    vendor_id = body["vendor_id"]
    by_vendor = await db["resources"].find_one(
        {
            "project_id": project_id,
            "resource_type": resource_type,
            "vendor_id": vendor_id,
        }
    )
    if by_vendor is not None:
        by_vendor_body: dict[str, Any] = by_vendor["body"]
        return by_vendor_body

    resource = {**body, "in_progress": body.get("in_progress", True)}
    await db["resources"].insert_one(
        {
            "idempotency_key": idempotency_key,
            "project_id": project_id,
            "resource_type": resource_type,
            "vendor_id": vendor_id,
            "body": resource,
        }
    )
    return resource


@app.patch("/projects/{project_id}/project_resources/{resource_type}/{vendor_id}")
async def update_resource(
    project_id: str,
    resource_type: str,
    vendor_id: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    row = await db["resources"].find_one(
        {
            "project_id": project_id,
            "resource_type": resource_type,
            "vendor_id": vendor_id,
        }
    )
    if row is None:
        raise HTTPException(404, detail=f"resource '{vendor_id}' not found")
    updated_body = {**row["body"], **body}
    update: dict[str, Any] = {"body": updated_body}
    # A finalize may carry the real vendor id the engine provisioned; re-key the record to it
    # so it is findable under its true identity (the run-id was only a create-time placeholder).
    new_vendor_id = body.get("vendor_id")
    if new_vendor_id:
        update["vendor_id"] = new_vendor_id
    await db["resources"].update_one({"_id": row["_id"]}, {"$set": update})
    return updated_body


@app.get("/projects/{project_id}/project_resources/{resource_type}")
async def list_resources(project_id: str, resource_type: str) -> list[dict[str, Any]]:
    """Convenience for eyeballing state during development."""
    rows = (
        await db["resources"]
        .find({"project_id": project_id, "resource_type": resource_type})
        .to_list(length=None)
    )
    return [r["body"] for r in rows]
