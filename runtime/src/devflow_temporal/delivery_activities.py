"""Effectful activities used by the managed Temporal delivery protocol."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from temporalio import activity

from .candidate import candidate_for
from .contracts import digest
from .delivery_broker import DeliveryBroker
from .delivery_config import DeliveryConfig
from .delivery_store import DeliveryStore
from .supervisor import get_supervisor


def _context(spec: dict[str, Any]) -> tuple[DeliveryStore, DeliveryBroker]:
    config = DeliveryConfig.load(Path(spec["config_path"]))
    if digest(config.raw) != spec["config_digest"]:
        raise ValueError("service configuration changed during an active run")
    store = DeliveryStore(config)
    saved = store.spec(spec["run_id"])
    if saved != spec:
        raise ValueError("Temporal input no longer matches the durable submitted run")
    return store, DeliveryBroker(store, spec)


@activity.defn(name="delivery_project")
async def delivery_project(request: dict[str, Any]) -> dict[str, Any]:
    store, _ = _context(request["spec"])
    result = store.project(
        request["spec"]["run_id"],
        phase=request["phase"],
        execution_state=request["execution_state"],
        event_type=request["event_type"],
        message=request["message"],
        candidate=request.get("candidate"),
        pull_request=request.get("pull_request"),
        checks=request.get("checks"),
        tracker=request.get("tracker"),
        usage=request.get("usage"),
        protocol_revision=request.get("protocol_revision"),
        outcome=request.get("outcome"),
        error=request.get("error"),
        key=request.get("key"),
    )
    return {"revision": result["revision"], "phase": result["phase"]}


@activity.defn(name="delivery_prepare")
async def delivery_prepare(request: dict[str, Any]) -> dict[str, Any]:
    _, broker = _context(request["spec"])
    return broker.prepare()


@activity.defn(name="delivery_role")
async def delivery_role(request: dict[str, Any]) -> dict[str, Any]:
    store, broker = _context(request["spec"])
    role = request["role"]
    iteration = request["iteration"]
    candidate = request["candidate"]
    if role == "implement":
        workspace = broker.checkout
        if broker.candidate() != candidate:
            raise ValueError("implementer checkout changed before its role")
    elif role in {"review", "verify"}:
        workspace = broker.gate_checkout(role, iteration, candidate)
    else:
        raise ValueError("unknown delivery role")
    result = await get_supervisor(store).run({**request, "workspace": str(workspace)})
    if role == "implement":
        after = broker.candidate()
        if result.get("status") == "pass" and after["id"] == candidate["id"]:
            result["status"] = "blocked"
            result.setdefault("findings", []).append("implementer produced no candidate change")
    else:
        observed = candidate_for(workspace)
        source = broker.candidate()
        if observed["id"] != candidate["id"] or source != candidate:
            result["status"] = "blocked"
            result.setdefault("findings", []).append("candidate changed during independent gate")
        after = candidate
    return {
        **result,
        "role": role,
        "iteration": iteration,
        "input_candidate_id": candidate["id"],
        "candidate": after,
        "provider": request["spec"]["provider"],
    }


@activity.defn(name="delivery_publish")
async def delivery_publish(request: dict[str, Any]) -> dict[str, Any]:
    _, broker = _context(request["spec"])
    return broker.publish(request["iteration"], request["candidate"])


@activity.defn(name="delivery_checks")
async def delivery_checks(request: dict[str, Any]) -> dict[str, Any]:
    _, broker = _context(request["spec"])
    return broker.run_checks(request["iteration"], request["candidate"])


@activity.defn(name="delivery_precheck")
async def delivery_precheck(request: dict[str, Any]) -> dict[str, Any]:
    _, broker = _context(request["spec"])
    return broker.run_prechecks(request["iteration"], request["candidate"])


@activity.defn(name="delivery_ci")
async def delivery_ci(request: dict[str, Any]) -> dict[str, Any]:
    _, broker = _context(request["spec"])
    return await broker.checks(request["pull_request"])


def _tracker_sync(spec: dict[str, Any], status: str, *, release: bool) -> dict[str, Any]:
    store, _ = _context(spec)
    repository = store.config.raw["repositories"][spec["repository_key"]]
    project = repository.get("project_url")
    assignee = repository.get("assignee")
    if not project or not assignee:
        return {"state": "unconfigured", "reason": "tracker project or assignee is missing"}
    script = store.config.helpers_dir / "github.py"
    owner = f"external:devflow:{spec['run_id']}"
    command = [
        sys.executable,
        str(script),
        "--db",
        str(store.config.tracking_db),
        "set",
        "--work-id",
        spec["work_id"],
        "--owner",
        owner,
        "--assignee",
        assignee,
        "--project",
        project,
        "--status",
        status,
    ]
    if release:
        command.append("--release")
    result = subprocess.run(command, text=True, capture_output=True, check=False, timeout=120)
    if result.returncode:
        return {"state": "pending", "reason": (result.stderr or result.stdout).strip()[:500]}
    audit = subprocess.run(
        [
            sys.executable,
            str(script),
            "--db",
            str(store.config.tracking_db),
            "audit",
            "--work-id",
            spec["work_id"],
        ],
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    if audit.returncode:
        return {"state": "pending", "reason": (audit.stderr or audit.stdout).strip()[:500]}
    observed = json.loads(audit.stdout)
    expected_claim = not release
    if observed.get("state") != "consistent" or bool(observed.get("claim")) != expected_claim:
        return {"state": "pending", "observed": observed}
    return {"state": "consistent", "observed": observed}


@activity.defn(name="delivery_tracker_start")
async def delivery_tracker_start(request: dict[str, Any]) -> dict[str, Any]:
    if request["spec"]["provider"] == "fake":
        return {"state": "consistent", "observed": {"fixture": True}}
    return _tracker_sync(request["spec"], "in-progress", release=False)


@activity.defn(name="delivery_tracker")
async def delivery_tracker(request: dict[str, Any]) -> dict[str, Any]:
    return _tracker_sync(request["spec"], "in-review", release=True)


DELIVERY_ACTIVITIES = [
    delivery_project,
    delivery_prepare,
    delivery_role,
    delivery_publish,
    delivery_checks,
    delivery_precheck,
    delivery_ci,
    delivery_tracker_start,
    delivery_tracker,
]
