"""Feature coordinator activities, backed by execution journals and GitHub records."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
import subprocess
import tempfile
import time
from pathlib import Path

from temporalio import activity

from .contracts import digest
from .delivery_broker import _git
from .delivery_execution_registry import OwnershipConflict
from .delivery_feature_execution import (
    plan_for,
    register_worker,
    registry,
    worker_key,
    worker_spec,
)
from .delivery_feature_pass import checkpoint_key
from .delivery_feature_pass import checkpoints as current_checkpoints
from .delivery_feature_publication import current_record, live_members
from .delivery_github_contract import GitHubDelivery, ordered_chunks


def _context(spec):
    from .delivery_activities import _context as context

    return context(spec)


def _feature_context(spec):
    store, broker = _context(spec)
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    return store, broker, shared, token


def open_feature(spec):
    store, _, shared, token = _feature_context(spec)
    gh = GitHubDelivery()
    issue = spec["feature_delivery"]["snapshot"]["issue"]
    shared.checkpoint(token, "accepted-plan", {"digest": digest(plan_for(spec))})
    record = gh.initialize(issue, plan_for(spec), shared, token)
    shared.checkpoint(
        token, "github-record", {key: record[key] for key in ("comment_id", "comment_node_id")}
    )
    record = gh.workstreams(issue, record, shared, token)
    shared.checkpoint(token, "workstream-issues", record["manifest"]["workstream_issues"])
    live_members(spec, record, gh)
    checkpoints = current_checkpoints(spec)
    for member in record["manifest"]["publication"]["members"]:
        proof = checkpoints.get("verified:" + member["chunk_id"])
        if proof and (proof["head"] != member["head"] or proof["number"] != member["number"]):
            raise OwnershipConflict("verified chunk no longer matches its recorded publication")
    return {"record": record, "checkpoints": checkpoints, "budget": shared.budget(issue["id"])}


@activity.defn(name="delivery_feature_open")
async def delivery_feature_open(request):
    return await asyncio.to_thread(open_feature, request["spec"])


def reserve(spec, chunk_id, kind):
    store, _, shared, token = _feature_context(spec)
    record = current_record(spec)
    plan = record["manifest"]["plan"]
    chunk = next((item for item in ordered_chunks(plan) if item["id"] == chunk_id), None)
    if chunk is None or kind not in {"build", "chunk"}:
        raise ValueError("unknown feature worker assignment")
    checkpoints = current_checkpoints(spec)
    if any("verified:" + key not in checkpoints for key in chunk["depends_on"]):
        raise OwnershipConflict("worker's prerequisites have not passed their integrated gates")
    assignment_key = f"assignment:{chunk_id}:{kind}"
    previous = checkpoints.get(assignment_key)
    if previous:
        if previous["store_path"] != str(store.config.tracking_db):
            raise OwnershipConflict("continue this feature on the runtime that owns its workers")
        child = store.effective_spec(previous["run_id"])
        with store._connect() as db:
            row = dict(
                db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id=?", (child["run_id"],)
                ).fetchone()
            )
        if row["outcome"] == "delivered":
            return {"spec": child, "completed": True, "workflow_id": row["workflow_id"]}
        if row["outcome"] in {"blocked", "cancelled"}:
            return resume_worker(store, spec, child, row)
        shared.reserve_worker(token, worker_key(child["run_id"], token), chunk["workstream_id"])
        return {"spec": child, "completed": False, "workflow_id": row["workflow_id"]}
    members = record["manifest"]["publication"]["members"]
    integration = checkpoints["integration-pass"]
    index = next(i for i, item in enumerate(ordered_chunks(plan)) if item["id"] == chunk_id)
    if kind == "chunk":
        if not integration and any(member["chunk_id"] == chunk_id for member in members):
            raise OwnershipConflict("published chunk lost its execution assignment")
        if not integration and ordered_chunks(plan)[len(members)]["id"] != chunk_id:
            raise OwnershipConflict("chunk integration is not next in the recorded stack")
        if integration and any("verified:" + item["id"] not in checkpoints
                               for item in ordered_chunks(plan)[:index]):
            raise OwnershipConflict("integration pass must reverify every preceding stack layer")
    head = members[-1]["head"] if members else spec["base_sha"]
    branch = members[-1]["branch"] if members else spec["publication_base_ref"]
    if kind == "chunk" and integration:
        head = members[index - 1]["head"] if index else integration["target"]
        branch = members[index - 1]["branch"] if index else spec["publication_base_ref"]
    seed = checkpoints.get("build:" + chunk_id) if kind == "chunk" else None
    if kind == "chunk" and seed is None:
        raise OwnershipConflict("chunk lacks its preserved implementation checkpoint")
    child = worker_spec(
        spec,
        chunk,
        record["manifest"]["workstream_issues"][chunk["workstream_id"]],
        kind=kind,
        base_sha=head,
        base_branch=branch,
        seed=seed,
    )
    # Ensure the exact remote prerequisite object is available locally before
    # admitting a checkout. Never move the configured source working tree.
    _git(Path(spec["source_path"]), "fetch", "--no-tags", "origin", branch)
    if _git(Path(spec["source_path"]), "rev-parse", "FETCH_HEAD") != head:
        raise OwnershipConflict("stack prerequisite moved before worker admission")
    register_worker(store, spec, child)
    shared.checkpoint(
        token,
        checkpoint_key(child, assignment_key),
        {
            "run_id": child["run_id"],
            "store_path": str(store.config.tracking_db),
            "chunk_id": chunk_id,
            "kind": kind,
        },
    )
    return {"spec": child, "completed": False, "workflow_id": "delivery-" + child["run_id"]}


def begin_integration(spec, target):
    store, _, shared, token = _feature_context(spec)
    with shared.mutation(token):
        record = current_record(spec)
        live = live_members(spec, record)
        if not live or any(raw["merged"] or raw["state"] != "open" for raw in live):
            raise OwnershipConflict("integration requires the existing open stack")
        if live[0]["base"]["sha"] != target:
            raise OwnershipConflict("target moved again before integration admission")
        prior = current_checkpoints(spec)["integration-pass"]
        if prior and prior["target"] == target:
            return prior
        with shared.connect() as db:
            shared._settled(db, token["issue_id"])
        value = {"number": prior["number"] + 1 if prior else 1, "target": target,
                 "members": record["manifest"]["publication"]["members"]}
        shared.repair(token, "integration-pass:" + str(value["number"]),
                      "Reintegrating feature on updated target")
        shared.checkpoint(token, "integration-pass:" + str(value["number"]), value)
        return value


@activity.defn(name="delivery_feature_begin_integration")
async def delivery_feature_begin_integration(request):
    return await asyncio.to_thread(begin_integration, request["spec"], request["target"])


def resume_worker(store, parent, child, row):
    from . import delivery_gate_retry as gates
    from .delivery_stopped_resume import admit, pending_repair, snapshot

    shared = registry(parent)
    token = parent["feature_delivery"]["owner"]
    # The existing supported recovery reads Temporal closure and native cleanup,
    # preserves the original candidate/PR/session and never creates a new branch.
    preparation_failed = gates.prepublication_preparation_failed(
        {"error": row["error"], "checks": json.loads(row["checks_json"] or "{}")})
    if preparation_failed:
        # The selector is only a hint. Gate admission authenticates the complete
        # closed result, candidate, failed check, cleanup, and bounded retry history.
        sealed = gates.snapshot(store, child["run_id"], gates.PREPUBLICATION_KIND)
        state = sealed["closed"]["result"]
        remaining = 0
        kind = gates.PREPUBLICATION_KIND
        admit = gates.admit
    else:
        sealed = snapshot(store, child["run_id"])
        state = sealed["state"]
        budget = shared.budget(token["issue_id"])
        remaining = min(budget["maximum"] - budget["used"],
                        child["policy"]["max_repairs"] - state["iteration"])
        if remaining < 0 or (remaining == 0 and not pending_repair(sealed)):
            raise OwnershipConflict("feature product repair limit exhausted")
        if remaining == 0:
            # A zero-additional admission may only finish a cycle already charged
            # to this worker, including after another workstream used the balance.
            with shared.connect() as db:
                shared.require(db, token)
                paid = db.execute(
                    "SELECT reason FROM execution_repairs WHERE issue_id=? AND repair_key=?",
                    (token["issue_id"], f"{child['run_id']}:{state['iteration']}"),
                ).fetchone()
            if not paid or paid["reason"] != (
                    "Product repair at worker iteration " + str(state["iteration"])):
                raise OwnershipConflict("pending repair has no matching product allowance debit")
        kind = "stopped_delivery_resume"
    candidate = sealed["candidate"]
    command = {
        "continuation_kind": kind,
        "command_id": "feature-resume-" + digest({"owner": token, "run": child["run_id"]})[:24],
        "expected_revision": state["revision"],
        "expected_iteration": state["iteration"],
        "expected_candidate_id": candidate["id"],
        "expected_candidate_head": candidate["head"],
        "additional_iterations": remaining,
    }
    shared.reserve_worker(
        token, worker_key(child["run_id"], token), child["feature_worker"]["workstream_id"]
    )
    result = admit(store, child["run_id"], command)
    return {
        "spec": store.effective_spec(child["run_id"]),
        "completed": False,
        "workflow_id": result["workflow_id"],
        "resumed": True,
    }


@activity.defn(name="delivery_feature_reserve")
async def delivery_feature_reserve(request):
    return await asyncio.to_thread(reserve, request["spec"], request["chunk_id"], request["kind"])


def seal_build(spec, candidate):
    store, broker = _context(spec)
    shared = registry(spec)
    token = shared.token(shared.current(spec["feature_delivery"]["owner"]["issue_id"]))
    chunk_id = spec["feature_worker"]["chunk_id"]
    prior = shared.checkpoints(token["issue_id"]).get("build:" + chunk_id)
    if prior:
        if hashlib.sha256(Path(prior["path"]).read_bytes()).hexdigest() != prior["sha256"]:
            raise OwnershipConflict("preserved worker checkpoint changed")
        return prior
    if broker.candidate() != candidate:
        raise OwnershipConflict("worker source changed before its integration checkpoint")
    changed = sorted(broker.validate_candidate_scope())
    if changed:
        _git(broker.checkout, "add", "--", *changed)
    patch = subprocess.run(
        [
            "git",
            "-C",
            str(broker.checkout),
            "diff",
            "--cached",
            "--binary",
            "--full-index",
            spec["base_sha"],
            "--",
        ],
        check=True,
        capture_output=True,
        timeout=30,
    ).stdout
    path = Path(spec["state_dir"]) / "feature-implementation.patch"
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists() or path.is_symlink():
        # Atomic creation below makes only complete files observable on replay.
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
                or path.read_bytes() != patch):
            raise OwnershipConflict("preserved implementation patch differs from this candidate")
    else:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
            staged = Path(stream.name)
            stream.write(patch)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(staged, 0o600)
        os.link(staged, path)
        staged.unlink()
    result = {
        "path": str(path),
        "sha256": hashlib.sha256(patch).hexdigest(),
        "base": spec["base_sha"],
        "candidate": candidate,
        "worker_run_id": spec["run_id"],
        "paths": changed,
    }
    shared.checkpoint(token, "build:" + chunk_id, result)
    return result


@activity.defn(name="delivery_feature_seal_build")
async def delivery_feature_seal_build(request):
    return await asyncio.to_thread(seal_build, request["spec"], request["candidate"])


def apply_seed(broker):
    seed = broker.spec.get("feature_worker", {}).get("seed")
    if seed is None:
        return
    from .delivery_resources import read_private, write_private

    shared = registry(broker.spec)
    issue_id = broker.spec["feature_delivery"]["owner"]["issue_id"]
    if (
        shared.checkpoints(issue_id).get("build:" + broker.spec["feature_worker"]["chunk_id"])
        != seed
    ):
        raise OwnershipConflict("integration input is not the sealed worker checkpoint")
    data = Path(seed["path"]).read_bytes()
    if hashlib.sha256(data).hexdigest() != seed["sha256"]:
        raise OwnershipConflict("integration checkpoint bytes changed")
    receipt = broker.state_dir / "feature-seed.json"
    if receipt.exists():
        if read_private(receipt)["seed"] != seed:
            raise OwnershipConflict("checkout already imported different work")
        return
    if _git(broker.checkout, "status", "--porcelain"):
        raise OwnershipConflict(
            "integration import was interrupted; preserve and reconcile checkout"
        )
    result = (
        subprocess.run(
            ["git", "-C", str(broker.checkout), "apply", "--3way", "--index", seed["path"]],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        if data
        else None
    )
    broker.validate_candidate_scope()
    write_private(
        receipt,
        {
            "seed": seed,
            "applied": result is None or result.returncode == 0,
            "diagnostic": (result.stderr[-2000:] if result else ""),
        },
    )


def integrate_previous(broker):
    """Retain the exact old PR head as a parent; publishing never force-pushes."""
    from .delivery_resources import read_private, write_private

    previous = broker.spec["feature_worker"]["previous_publication"]
    receipt = broker.state_dir / "feature-reintegration.json"
    expected = {"previous": previous, "base": broker.spec["base_sha"]}
    if receipt.exists():
        if read_private(receipt)["input"] != expected:
            raise OwnershipConflict("integration checkout is bound to another stack head")
        return
    _git(broker.source, "fetch", "--no-tags", "origin", previous["branch"])
    if _git(broker.source, "rev-parse", "FETCH_HEAD") != previous["head"]:
        raise OwnershipConflict("recorded PR head moved before integration")
    if _git(broker.checkout, "rev-parse", "HEAD") != broker.spec["base_sha"]:
        raise OwnershipConflict("new integration checkout moved before import")
    pending = subprocess.run(
        ["git", "-C", str(broker.checkout), "rev-parse", "--verify", "MERGE_HEAD"],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if pending.returncode == 0:
        if pending.stdout.strip() != previous["head"]:
            raise OwnershipConflict("checkout has a different unfinished merge")
        diagnostic = "Recovered original integration after its receipt was lost"
    else:
        if _git(broker.checkout, "status", "--porcelain"):
            raise OwnershipConflict("new integration checkout has unattributed changes")
        result = subprocess.run(
            ["git", "-C", str(broker.checkout), "-c", "core.hooksPath=/dev/null",
             "merge", "--no-commit", "--no-ff", previous["head"]],
            capture_output=True, text=True, check=False, timeout=60,
        )
        diagnostic = result.stderr[-2000:]
        if result.returncode and not _git(
                broker.checkout, "diff", "--name-only", "--diff-filter=U"):
            raise OwnershipConflict("original PR could not be imported: " + diagnostic)
    broker.validate_candidate_scope()
    write_private(receipt, {"input": expected, "diagnostic": diagnostic})


def finish_worker(spec, child_id):
    store, _, shared, token = _feature_context(spec)
    child = store.effective_spec(child_id)
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (child_id,)).fetchone())
        attempts = [
            dict(item)
            for item in db.execute(
                "SELECT state,cleanup FROM delivery_attempts WHERE run_id=?", (child_id,)
            )
        ]
    if row["outcome"] not in {"delivered", "blocked", "cancelled"}:
        raise OwnershipConflict("worker is still running")
    if any(item["state"] != "finished" or item["cleanup"] != "confirmed" for item in attempts):
        raise OwnershipConflict("worker has unfinished native attempts")
    checks = json.loads(row["checks_json"] or "{}")
    if child["provider"] == "codex":
        from .delivery_resources import observe_finalized_resources

        observe_finalized_resources(child)
        if checks.get("resource_cleanup", {}).get("state") != "confirmed":
            raise OwnershipConflict("worker resource cleanup remains unresolved")
    receipt = {"run_id": child_id, "outcome": row["outcome"], "cleanup": "confirmed"}
    with store._connect() as db:
        claim = store.state.claim_for(db, child["work_id"])
        if claim:
            store.state.release_work(db, child["work_id"], "external:devflow:" + child_id)
    # Reusing a fully verified worker on a continuation needs no new live lease.
    with shared.connect() as db:
        assigned = db.execute(
            "SELECT 1 FROM execution_workers WHERE issue_id=? AND worker_key=?",
            (token["issue_id"], worker_key(child_id, token)),
        ).fetchone()
    if assigned:
        shared.finish_worker(token, worker_key(child_id, token), receipt)
    if row["outcome"] == "delivered" and child["feature_worker"]["kind"] == "chunk":
        pr = json.loads(row["pr_json"])
        record = current_record(spec)
        live_members(spec, record)
        member = next(
            item
            for item in record["manifest"]["publication"]["members"]
            if item["chunk_id"] == child["feature_worker"]["chunk_id"]
        )
        if member["head"] != pr["head"]:
            raise OwnershipConflict("verified worker no longer owns the published head")
        shared.checkpoint(
            token,
            checkpoint_key(child, "verified:" + child["feature_worker"]["chunk_id"]),
            {
                "run_id": child_id,
                "head": pr["head"],
                "number": pr["number"],
                "checks_digest": digest(checks),
                "store_path": str(store.config.tracking_db),
            },
        )
    return {**receipt, "record": current_record(spec), "budget": shared.budget(token["issue_id"])}


@activity.defn(name="delivery_feature_finish_worker")
async def delivery_feature_finish_worker(request):
    return await asyncio.to_thread(finish_worker, request["spec"], request["child_id"])


def stop_feature(spec, checkpoint):
    from .delivery_activities import _context as context

    store, _ = context(spec, allow_stopped_feature=True)
    shared, token = registry(spec), spec["feature_delivery"]["owner"]
    # Normal workflow termination is not enough: every worker and remote intent
    # must be reconciled before the owner can become available to a successor.
    if spec["provider"] == "codex":
        from .delivery_resources import observe_finalized_resources

        observe_finalized_resources(spec)
    shared.stop(token, "stopped:" + str(token["generation"]), checkpoint)
    with store._connect() as db:
        claim = store.state.claim_for(db, spec["work_id"])
        if claim:
            store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
    return {"owner": token, "state": "stopped"}


@activity.defn(name="delivery_feature_stop")
async def delivery_feature_stop(request):
    return await asyncio.to_thread(stop_feature, request["spec"], request["checkpoint"])


@activity.defn(name="delivery_feature_worker_result")
async def delivery_feature_worker_result(request):
    store, _, _, _ = await asyncio.to_thread(_feature_context, request["spec"])
    child_id = request["child_id"]
    with store._connect() as db:
        row = db.execute(
            "SELECT outcome,workflow_id FROM delivery_runs WHERE run_id=?", (child_id,)
        ).fetchone()
    if not row or row["outcome"] is None:
        return {"closed": False}
    try:
        result = await asyncio.to_thread(
            store._completed_temporal_result,
            child_id,
            workflow_id=store.active_workflow_id(child_id),
        )
    except ValueError as exc:
        if str(exc) != "continuation predecessor Temporal closure is unproven":
            raise
        return {"closed": False, "reason": "temporal_closure_pending"}
    return {"closed": True, "outcome": result.get("result", {}).get("outcome", row["outcome"])}


@activity.defn(name="delivery_feature_settle_workers")
async def delivery_feature_settle_workers(request):
    from temporalio.client import Client, WorkflowExecutionStatus, WorkflowUpdateFailedError
    from temporalio.service import RPCError, RPCStatusCode

    from .delivery_codec import DELIVERY_DATA_CONVERTER

    store, _, shared, token = await asyncio.to_thread(_feature_context, request["spec"])
    shared.drain(token)
    client = await Client.connect(
        store.config.temporal_address,
        namespace=store.config.raw.get("temporal_namespace", "default"),
        data_converter=DELIVERY_DATA_CONVERTER,
    )
    deadline = time.monotonic() + 7200
    while time.monotonic() < deadline:
        with shared.connect() as db:
            rows = [
                dict(item)
                for item in db.execute(
                    "SELECT * FROM execution_workers WHERE issue_id=? AND generation=? "
                    "AND state!='finished'",
                    (token["issue_id"], token["generation"]),
                )
            ]
        if not rows:
            return {"state": "confirmed"}
        for worker in rows:
            child_id = worker["worker_key"].split(":generation:")[0]
            with store._connect() as db:
                child = db.execute(
                    "SELECT run_id FROM delivery_runs WHERE run_id=?", (child_id,)
                ).fetchone()
            if not child:
                saved = shared.checkpoints(token["issue_id"]).get(
                    "worker-input:" + worker["worker_key"]
                )
                if saved is None:
                    raise OwnershipConflict("worker admission input is unconfirmed")
                await asyncio.to_thread(register_worker, store, request["spec"], saved)
            handle = client.get_workflow_handle(store.active_workflow_id(child_id))
            try:
                remote = await handle.describe()
            except RPCError as exc:
                if exc.status != RPCStatusCode.NOT_FOUND:
                    raise
                # Admission precedes dispatch. The coordinator's worker task may
                # still be acknowledging its start; absence is not cleanup proof.
                continue
            if remote.status == WorkflowExecutionStatus.RUNNING:
                state = await handle.query("status")
                if state.get("outcome") is None and not state.get("checks", {}).get(
                    "terminal_tracker_checkpoint"
                ):
                    try:
                        await handle.execute_update(
                            "cancel",
                            {
                                "command_id": (
                                    "feature-stop-"
                                    + digest(token)[:16]
                                    + "-"
                                    + child_id
                                    + "-"
                                    + str(state["revision"])
                                ),
                                "expected_revision": state["revision"],
                                "reason": "Coordinator stopping; preserve the feature checkpoint",
                            },
                        )
                    except WorkflowUpdateFailedError:
                        # A concurrently completed role may have advanced the
                        # revision. Re-read on the next bounded settlement pass.
                        continue
            else:
                try:
                    await asyncio.to_thread(finish_worker, request["spec"], child_id)
                except (ValueError, OwnershipConflict) as exc:
                    from temporalio.exceptions import ApplicationError

                    owner = shared.current(token["issue_id"])
                    if owner and shared.token(owner) == token and owner["state"] == "draining":
                        raise ApplicationError(
                            "Worker settlement remains pending: " + str(exc)[:300],
                            {"owner": token, "worker": child_id},
                            type="FeatureWorkerSettlementPending", non_retryable=True,
                        ) from exc
                    raise
        activity.heartbeat({"stage": "settling_feature_workers", "remaining": len(rows)})
        await asyncio.sleep(5)
    raise OwnershipConflict("worker closure remains unresolved; feature ownership is retained")


@activity.defn(name="delivery_feature_merge")
async def delivery_feature_merge(request):
    from threading import Event

    from .delivery_activities import _with_heartbeat
    from .delivery_feature_merge import merge

    store, _ = await asyncio.to_thread(_context, request["spec"])
    cancelled = Event()
    try:
        return await _with_heartbeat(
            asyncio.to_thread(
                merge,
                store,
                request["spec"],
                request["publication"],
                request.get("authorization"),
                cancelled=cancelled,
            ),
            request,
            "feature_merge",
        )
    finally:
        cancelled.set()


@activity.defn(name="delivery_feature_settle_effects")
async def delivery_feature_settle_effects(request):
    from .delivery_activities import _with_heartbeat
    from .delivery_feature_readback import settle

    store, _, _, _ = await asyncio.to_thread(_feature_context, request["spec"])
    return await _with_heartbeat(asyncio.to_thread(settle, store, request["spec"]),
                                 request, "feature_readback")


FEATURE_ACTIVITIES = [
    delivery_feature_open,
    delivery_feature_reserve,
    delivery_feature_seal_build,
    delivery_feature_finish_worker,
    delivery_feature_stop,
    delivery_feature_worker_result,
    delivery_feature_settle_workers,
    delivery_feature_merge,
    delivery_feature_begin_integration,
    delivery_feature_settle_effects,
]
