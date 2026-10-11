"""Bounded, journalled corrections to GitHub-owned plans.

GitHub remains the definition owner. These checkpoints record proposal custody,
read-back adoption and immutable execution overlays, never a replacement feature
catalog. Every attempt shares the existing cumulative product repair allowance.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from copy import deepcopy
from datetime import datetime
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_config import COMMAND_RE
from .delivery_execution_registry import OwnershipConflict

PREFIX = "plan-revision:"
DIAGNOSTIC_FIELDS = {
    "version",
    "kind",
    "category",
    "chunk_id",
    "plan_revision",
    "plan_sha256",
    "candidate_id",
    "evidence",
    "detail",
}
CATEGORIES = {"assumption", "decomposition", "dependency", "gate_prerequisite"}
SHA256 = re.compile(r"[0-9a-f]{64}")


def _registry(spec, shared=None):
    from .delivery_feature_execution import registry

    return shared or registry(spec)


def _values(spec, shared=None):
    return _registry(spec, shared).checkpoints(spec["feature_delivery"]["owner"]["issue_id"])


def _latest(values):
    rows = [value for key, value in values.items() if key.startswith(PREFIX + "adopted:")]
    return max(rows, key=lambda item: item["identity"]["plan_revision"]) if rows else None


def adopted_plan(spec, shared=None):
    if not spec.get("feature_delivery"):
        return None
    receipt = _latest(_values(spec, shared))
    return deepcopy(receipt["plan"]) if receipt else None


def adopted_plan_identity(spec, shared=None):
    """Current exact remote definition identity, with unchanged legacy fallback."""
    values = _values(spec, shared)
    receipt = _latest(values)
    if receipt:
        return deepcopy(receipt["identity"])
    feature = spec["feature_delivery"]
    record = feature["snapshot"].get("delivery") or {}
    manifest = record.get("manifest") or {}
    reference = values.get("github-record") or record
    plan = manifest.get("plan")
    if plan is None and spec.get("accepted_plan") and not spec.get("feature_worker"):
        from .delivery_feature_execution import plan_for

        plan = plan_for(spec)
    accepted = values.get("accepted-plan") or {}
    return {
        "plan_revision": manifest.get("plan_revision", 1),
        "plan_digest": digest(plan) if plan is not None else accepted.get("digest"),
        "comment_id": reference.get("comment_id"),
        "comment_node_id": reference.get("comment_node_id"),
        "workstream_issues": deepcopy(
            values.get("workstream-issues") or manifest.get("workstream_issues") or {}
        ),
    }


def effective_spec(store, spec):
    """Authenticate an appended plan overlay while retaining the original run input."""
    if not spec.get("feature_delivery") or spec.get("feature_worker"):
        return spec
    values = _values(spec)
    overlays = [
        value
        for key, value in values.items()
        if key.startswith(PREFIX + "spec:" + spec["run_id"] + ":")
    ]
    if not overlays:
        return spec
    overlay = max(overlays, key=lambda value: value["identity"]["plan_revision"])
    revised = overlay["spec"]
    if (
        overlay["spec_digest"] != digest(revised)
        or revised["feature_delivery"]["owner"] != spec["feature_delivery"]["owner"]
        or any(
            revised.get(key) != spec.get(key)
            for key in (
                "run_id",
                "work_id",
                "request_digest",
                "config_path",
                "config_digest",
                "state_dir",
                "checkout",
                "branch",
                "base_sha",
                "authorized_endpoint",
            )
        )
        or revised["policy"] != spec["policy"]
    ):
        raise OwnershipConflict("adopted plan execution overlay lost its original authority")
    return deepcopy(revised)


def revision_request(spec, shared=None):
    values = _values(spec, shared)
    explicit = spec.get("feature_plan_revision_request")
    if explicit:
        saved = values.get(PREFIX + "request:" + explicit["revision_id"])
        if saved != explicit:
            raise OwnershipConflict("revision request differs from its durable admission")
        return None if _terminal(values, explicit["revision_id"]) else deepcopy(explicit)
    pending = [
        value
        for key, value in values.items()
        if key.startswith(PREFIX + "begin:")
        and value["owner"] == spec["feature_delivery"]["owner"]
        and not _terminal(values, value["revision_id"])
    ]
    return deepcopy(pending[-1]) if pending else None


def _terminal(values, revision_id):
    return next(
        (
            value
            for key, value in values.items()
            if key.startswith((PREFIX + "adopted:", PREFIX + "rejected:"))
            and value.get("revision_id") == revision_id
        ),
        None,
    )


def _text(value, label, maximum=4000):
    if not isinstance(value, str) or not value.strip() or len(value) > maximum or "\x00" in value:
        raise ValueError(label + " must be nonempty bounded text")
    return value


def _evidence_shape(evidence):
    if (
        not isinstance(evidence, list)
        or len(evidence) > 16
        or any(
            not isinstance(item, dict)
            or set(item) != {"path", "sha256"}
            or not isinstance(item["path"], str)
            or not item["path"]
            or len(item["path"]) > 4096
            or not isinstance(item["sha256"], str)
            or not SHA256.fullmatch(item["sha256"])
            for item in evidence
        )
        or len({item["path"] for item in evidence}) != len(evidence)
    ):
        raise ValueError("revision evidence must name bounded unique path/digest receipts")
    return evidence


def _evidence(store, spec, items):
    """Read only existing owned run evidence and seal exact bytes, not caller assertions."""
    _evidence_shape(items)
    roots = {Path(spec["state_dir"])}
    values = _values(spec)
    for key, assignment in values.items():
        if "assignment:" not in key or not isinstance(assignment, dict):
            continue
        if assignment.get("store_path") != str(store.config.tracking_db):
            continue
        try:
            child = store.effective_spec(assignment["run_id"])
        except (KeyError, ValueError):
            continue
        if (
            child.get("feature_delivery", {}).get("owner", {}).get("issue_id")
            == spec["feature_delivery"]["owner"]["issue_id"]
        ):
            roots.add(Path(child["state_dir"]))
    result = []
    for item in items:
        path = Path(item["path"])
        if not path.is_absolute():
            path = Path(spec["state_dir"]) / path
        if (
            not any(path.is_relative_to(root) for root in roots)
            or path.resolve(strict=True) != path
        ):
            raise ValueError("revision evidence escaped retained run custody")
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or info.st_size > 16 * 1024 * 1024
        ):
            raise ValueError("revision evidence is not bounded owned regular data")
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != item["sha256"]:
            raise ValueError("revision evidence digest changed")
        result.append(
            {
                "path": str(path),
                "sha256": item["sha256"],
                "excerpt": content[:8192].decode("utf-8", errors="replace"),
            }
        )
    return result


def _assert_identity(record, identity):
    manifest = record["manifest"]
    observed = {
        "plan_revision": manifest.get("plan_revision", 1),
        "plan_digest": digest(manifest["plan"]),
        "comment_id": record["comment_id"],
        "comment_node_id": record["comment_node_id"],
        "workstream_issues": manifest["workstream_issues"],
    }
    if any(identity.get(key) is not None and identity[key] != observed[key] for key in observed):
        raise OwnershipConflict("GitHub plan revision or exact definition identities changed")
    return observed


def _worker_candidates(store, spec):
    values = _values(spec)
    result = {}
    for key, assignment in values.items():
        if "assignment:" not in key or not isinstance(assignment, dict):
            continue
        if assignment.get("store_path") != str(store.config.tracking_db):
            continue
        with store._connect() as db:
            row = db.execute(
                "SELECT candidate_json FROM delivery_runs WHERE run_id=?", (assignment["run_id"],)
            ).fetchone()
        if row:
            candidate = json.loads(row[0] or "null")
            if candidate:
                result.setdefault(assignment["chunk_id"], set()).add(candidate["id"])
    return result


def validate_diagnostic(store, spec, diagnostic, *, identity=None, candidate_id=None):
    from .delivery_feature_execution import plan_for
    from .delivery_github_contract import ordered_chunks

    if (
        not isinstance(diagnostic, dict)
        or set(diagnostic) != DIAGNOSTIC_FIELDS
        or diagnostic.get("version") != 1
        or diagnostic.get("kind") != "planning_defect"
        or diagnostic.get("category") not in CATEGORIES
        or type(diagnostic.get("plan_revision")) is not int
        or not isinstance(diagnostic.get("plan_sha256"), str)
        or not SHA256.fullmatch(diagnostic["plan_sha256"])
        or not isinstance(diagnostic.get("candidate_id"), str)
        or not SHA256.fullmatch(diagnostic["candidate_id"])
    ):
        raise ValueError("revision requires a structured candidate-bound planning defect")
    _text(diagnostic["detail"], "planning defect detail")
    current = identity or adopted_plan_identity(spec)
    if (
        diagnostic["plan_revision"] != current["plan_revision"]
        or diagnostic["plan_sha256"] != current["plan_digest"]
    ):
        raise OwnershipConflict("planning diagnostic refers to a stale plan revision")
    if diagnostic["chunk_id"] not in {item["id"] for item in ordered_chunks(plan_for(spec))}:
        raise ValueError("planning diagnostic names an unknown chunk")
    candidates = _worker_candidates(store, spec).get(diagnostic["chunk_id"], set())
    if candidate_id:
        candidates.add(candidate_id)
    if diagnostic["candidate_id"] not in candidates:
        raise OwnershipConflict("planning diagnostic candidate has no matching execution evidence")
    if not diagnostic["evidence"]:
        raise ValueError("planning defect requires durable evidence")
    _evidence(store, spec, diagnostic["evidence"])
    return deepcopy(diagnostic)


def revise_feature_plan(store, run_id, request):
    """Public stopped-feature admission; the runtime crafts and reviews the plan."""
    from .delivery_feature_closure import closed_coordinator
    from .delivery_feature_execution import continue_feature
    from .delivery_feature_publication import current_record, live_members

    if (
        not isinstance(request, dict)
        or not {"command_id", "expected_revision", "reason"} <= request.keys()
        or request.keys() - {"command_id", "expected_revision", "reason", "evidence"}
        or not isinstance(request.get("command_id"), str)
        or not COMMAND_RE.fullmatch(request["command_id"])
        or type(request.get("expected_revision")) is not int
        or request["expected_revision"] < 1
    ):
        raise ValueError(
            "plan revision requires a command ID, projection revision and bounded reason"
        )
    _text(request["reason"], "revision reason")
    _evidence_shape(request.get("evidence", []))
    command_digest = digest({"run_id": run_id, "request": request})
    with store._connect() as db:
        prior = db.execute(
            "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
            (request["command_id"],),
        ).fetchone()
        row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
    if prior:
        if prior[0] != command_digest:
            raise OwnershipConflict("revision command ID already belongs to different inputs")
        return json.loads(prior[1])
    spec = store.effective_spec(run_id)
    if not spec.get("feature_delivery") or spec.get("feature_worker"):
        raise ValueError("select a stopped feature coordinator to revise its plan")
    if (
        row is None
        or row["revision"] != request["expected_revision"]
        or row["outcome"] not in {"blocked", "cancelled"}
        or row["cleanup"] != "confirmed"
        or not store.owns_execution(spec)
    ):
        raise OwnershipConflict("feature is not at the specified stopped checkpoint")
    shared = _registry(spec)
    token = spec["feature_delivery"]["owner"]
    revision_id = "revision-" + command_digest[:24]
    inner = {
        "command_id": "revision-owner-" + command_digest[:32],
        "expected_revision": request["expected_revision"],
    }
    successor_id = "run-feature-" + digest({"run": run_id, "command": inner})[:20]
    saved = _values(spec).get(PREFIX + "request:" + revision_id)
    if saved:
        if saved["request_digest"] != command_digest or saved["request"] != request:
            raise OwnershipConflict("recorded revision admission changed")
        admission = saved
    else:
        closed = closed_coordinator(store, run_id)
        if closed["result"].get("cleanup") != "confirmed":
            raise OwnershipConflict("revision predecessor cleanup is unconfirmed")
        _evidence(store, spec, request.get("evidence", []))
        record = current_record(spec)
        live_members(spec, record)
        identity = _assert_identity(record, adopted_plan_identity(spec, shared))
        budget = shared.budget(token["issue_id"])
        if budget["used"] >= budget["maximum"]:
            raise OwnershipConflict("feature product repair limit exhausted")
        admission = {
            "revision_id": revision_id,
            "phase": "requested",
            "owner": token,
            "predecessor_run_id": run_id,
            "successor_id": successor_id,
            "request": deepcopy(request),
            "request_digest": command_digest,
            "old_identity": identity,
            "old_plan": record["manifest"]["plan"],
            "record": record,
            "predecessor_closure": {key: value for key, value in closed.items() if key != "result"},
        }
        with shared.serialized(token["issue_id"]):
            with store._connect() as db:
                current = db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)
                ).fetchone()
            if dict(current) != dict(row):
                raise OwnershipConflict("stopped feature changed before revision admission")
            shared.revision_checkpoint(
                token, PREFIX + "request:" + revision_id, admission, stopped=True
            )
    response = continue_feature(store, run_id, inner, _revision=admission)
    if response["run_id"] != successor_id:
        raise OwnershipConflict("revision continuation selected a different coordinator")
    receipt = {
        **response,
        "predecessor_run_id": run_id,
        "revision_id": revision_id,
        "revision_phase": "requested",
        "expected_plan_revision": admission["old_identity"]["plan_revision"],
        "affected_chunks": [],
        "repair_budget": shared.budget(token["issue_id"]),
    }
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute(
            "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
            (request["command_id"],),
        ).fetchone()
        if prior:
            if prior[0] != command_digest:
                raise OwnershipConflict("revision command ID already belongs to different inputs")
            return json.loads(prior[1])
        db.execute(
            "INSERT INTO delivery_commands VALUES (?,?,?,?)",
            (request["command_id"], run_id, command_digest, canonical_json(receipt)),
        )
    return receipt


def authenticate_revision_submission(store, spec, admission):
    if not isinstance(admission, dict) or not spec.get("feature_predecessor"):
        raise OwnershipConflict("revision continuation has no sealed predecessor")
    from .delivery_execution_registry import ExecutionRegistry
    from .delivery_feature_execution import registry_path

    shared = ExecutionRegistry(registry_path(store.config))
    predecessor = spec["feature_predecessor"]
    saved = shared.checkpoints(predecessor["issue_id"]).get(
        PREFIX + "request:" + admission["revision_id"]
    )
    if (
        saved != admission
        or admission["owner"] != predecessor
        or admission["successor_id"] != spec["run_id"]
        or digest(json.loads(spec["accepted_plan"])) != admission["old_identity"]["plan_digest"]
    ):
        raise OwnershipConflict("revision continuation changed its sealed plan admission")
    original = store.effective_spec(admission["predecessor_run_id"])
    if original["feature_delivery"]["owner"] != predecessor:
        raise OwnershipConflict("revision continuation changed original owner custody")
    spec["plan_approval"] = original.get("plan_approval", "required")
    spec["feature_plan_revision_request"] = deepcopy(admission)
    spec["feature_plan_revision"] = {
        key: admission["old_identity"][key] for key in ("plan_revision", "plan_digest")
    }


def authenticate_continued_plan(store, spec):
    """Carry original approval and adopted identity through a stopped continuation."""
    predecessor = spec.get("feature_predecessor")
    if not predecessor:
        return
    original = store.effective_spec(predecessor["run_id"])
    if original.get("feature_delivery", {}).get("owner") != predecessor:
        raise OwnershipConflict("feature continuation changed its original owner custody")
    shared = _registry(original)
    with shared.connect() as db:
        owner = shared.require(db, predecessor, active=False)
        if owner["state"] != "stopped":
            raise OwnershipConflict("feature continuation has no settled predecessor")
        shared._settled(db, predecessor["issue_id"])
    accepted = spec.get("accepted_plan")
    previous = original.get("accepted_plan")
    if (json.loads(accepted) if accepted else None) != (json.loads(previous) if previous else None):
        raise OwnershipConflict("feature continuation changed its accepted plan")
    spec["plan_approval"] = original.get("plan_approval", "required")
    receipt = _latest(_values(original, shared))
    if receipt:
        identity = {key: receipt["identity"][key] for key in ("plan_revision", "plan_digest")}
        if not accepted or digest(json.loads(accepted)) != identity["plan_digest"]:
            raise OwnershipConflict("feature continuation changed its accepted plan revision")
        spec["feature_plan_revision"] = identity


def authenticate_adopted_plan_custody(store, parent, receipt):
    """An accepted adoption survives only the registry's exact stopped-owner chain."""
    owner = parent["feature_delivery"]["owner"]
    previous = receipt["owner"]
    if owner == previous:
        return
    if owner["issue_id"] != previous["issue_id"] or owner["generation"] <= previous["generation"]:
        raise OwnershipConflict("adopted plan belongs to an unrelated execution owner")
    shared = _registry(parent)
    with shared.connect() as db:
        snapshots = [
            dict(row)
            for row in db.execute(
                "SELECT issue_id,run_id,store_path,generation FROM execution_snapshots "
                "WHERE issue_id=? AND generation>=? AND generation<=? ORDER BY generation",
                (owner["issue_id"], previous["generation"], owner["generation"]),
            )
        ]
    # claim() admits each next snapshot only from its exact stopped predecessor.
    # These append-only identities authenticate lineage without nesting old specs.
    if (
        len(snapshots) != owner["generation"] - previous["generation"] + 1
        or snapshots[0] != previous
        or snapshots[-1] != owner
        or [item["generation"] for item in snapshots]
        != list(range(previous["generation"], owner["generation"] + 1))
    ):
        raise OwnershipConflict("adopted plan has no exact retained continuation custody")


def _candidate(store, spec):
    from .delivery_broker import DeliveryBroker

    return DeliveryBroker(store, spec).candidate()


def _context(spec, revision_id):
    values = _values(spec)
    context = values.get(PREFIX + "begin:" + revision_id)
    if not context:
        raise OwnershipConflict("plan revision has no charged durable investigation")
    if context["owner"] != spec["feature_delivery"]["owner"]:
        raise OwnershipConflict("plan revision belongs to a different coordinator generation")
    terminal = _terminal(values, revision_id)
    return context, values, terminal


def planning_defect_repair_binding(spec, worker_run_id, iteration, diagnostic):
    """Bind a validated worker diagnosis to its already charged product cycle."""
    from .candidate import candidate_for
    from .delivery_feature_execution import worker_key

    worker = spec.get("feature_worker") or {}
    if (
        worker_run_id != spec.get("run_id")
        or not worker
        or type(iteration) is not int
        or iteration < 1
        or not isinstance(diagnostic, dict)
        or set(diagnostic) != DIAGNOSTIC_FIELDS
        or diagnostic.get("kind") != "planning_defect"
        or diagnostic.get("category") not in CATEGORIES
        or diagnostic.get("chunk_id") != worker.get("chunk_id")
    ):
        return None
    shared = _registry(spec)
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    current = shared.current(issue_id)
    if not current or current["state"] not in {"active", "draining"}:
        return None
    token = shared.token(current)
    identity = adopted_plan_identity(spec, shared)
    if (
        diagnostic.get("plan_revision") != identity["plan_revision"]
        or diagnostic.get("plan_sha256") != identity["plan_digest"]
        or diagnostic.get("candidate_id") != candidate_for(Path(spec["checkout"]))["id"]
    ):
        return None
    _text(diagnostic.get("detail"), "planning defect detail")
    if not _evidence_shape(diagnostic.get("evidence")):
        return None
    key = f"{worker_run_id}:{iteration}"
    with shared.connect() as db:
        charged = db.execute(
            "SELECT generation,reason FROM execution_repairs WHERE issue_id=? AND repair_key=?",
            (issue_id, key),
        ).fetchone()
        assigned = db.execute(
            "SELECT state,generation FROM execution_workers WHERE issue_id=? AND worker_key=?",
            (issue_id, worker_key(worker_run_id, token)),
        ).fetchone()
    if (
        not charged
        or charged["generation"] != token["generation"]
        or charged["reason"] != "Product repair at worker iteration " + str(iteration)
        or not assigned
        or assigned["state"] not in {"reserved", "running"}
        or assigned["generation"] != token["generation"]
    ):
        return None
    revision_id = (
        "revision-"
        + digest(
            {
                "owner": token,
                "diagnostic": diagnostic,
                "command_id": None,
            }
        )[:24]
    )
    return {"revision_id": revision_id, "repair_key": key}


def begin_revision(store, spec, diagnostic=None, command_id=None):
    """Seal and debit one proposal attempt before either configured role runs."""
    from .delivery_feature_publication import current_record, live_members

    shared = _registry(spec)
    token = spec["feature_delivery"]["owner"]
    public = revision_request(spec, shared)
    if diagnostic is None and not public:
        raise ValueError("autonomous replanning requires an evidenced planning defect")
    if command_id is not None and (
        not isinstance(command_id, str) or not COMMAND_RE.fullmatch(command_id)
    ):
        raise ValueError("revision attempt command identity is invalid")
    revision_id = (
        public["revision_id"]
        if public
        else "revision-"
        + digest({"owner": token, "diagnostic": diagnostic, "command_id": command_id})[:24]
    )
    values = _values(spec, shared)
    prior = values.get(PREFIX + "begin:" + revision_id)
    if prior:
        if prior["owner"] != token or (
            diagnostic is not None and prior["diagnostic"] != diagnostic
        ):
            raise OwnershipConflict("revision attempt identity already has different evidence")
        terminal = _terminal(values, revision_id)
        return {**deepcopy(prior), **({"phase": terminal["phase"]} if terminal else {})}
    record = current_record(spec)
    live_members(spec, record)
    identity = _assert_identity(record, adopted_plan_identity(spec, shared))
    candidate = _candidate(store, spec)
    if public and (
        identity != public["old_identity"]
        or record["manifest"]["publication"] != public["record"]["manifest"]["publication"]
    ):
        raise OwnershipConflict("GitHub changed after public revision admission")
    if diagnostic is not None:
        diagnostic = validate_diagnostic(
            store, spec, diagnostic, identity=identity, candidate_id=candidate["id"]
        )
    with shared.serialized(token["issue_id"]):
        with shared.connect() as db:
            shared.require_settlement(db, token)
            shared._settled(db, token["issue_id"])
            pending = shared._pending_revision(db, token["issue_id"])
        if pending and pending["revision_id"] != revision_id:
            raise OwnershipConflict("feature has a different proposal attempt in custody")
        begun = [
            value
            for key, value in _values(spec, shared).items()
            if key.startswith(PREFIX + "begin:")
            and not _terminal(_values(spec, shared), value["revision_id"])
        ]
        if any(value["revision_id"] != revision_id for value in begun):
            raise OwnershipConflict("feature has an active plan investigation")
        repair_key = "plan-revision:" + revision_id
        # Explicitly reuse a product cycle only when the diagnostic's exact worker
        # candidate has a documented debit; caller-supplied keys are never accepted.
        paid = None
        paid_worker = None
        if diagnostic:
            candidates = _worker_candidates(store, spec)
            for key, assignment in values.items():
                if (
                    "assignment:" not in key
                    or not isinstance(assignment, dict)
                    or assignment.get("chunk_id") != diagnostic["chunk_id"]
                ):
                    continue
                if diagnostic["candidate_id"] not in candidates.get(diagnostic["chunk_id"], set()):
                    continue
                with store._connect() as db:
                    worker = db.execute(
                        "SELECT iteration,checks_json,candidate_json FROM delivery_runs "
                        "WHERE run_id=?",
                        (assignment["run_id"],),
                    ).fetchone()
                if worker and (
                    json.loads(worker["candidate_json"] or "{}").get("id")
                    == diagnostic["candidate_id"]
                ):
                    pending_charge = json.loads(worker["checks_json"] or "{}").get(
                        "planning_defect_repair"
                    )
                    expected_key = f"{assignment['run_id']}:{worker['iteration']}"
                    if pending_charge == {"revision_id": revision_id, "repair_key": expected_key}:
                        with shared.connect() as db:
                            paid = db.execute(
                                "SELECT repair_key FROM execution_repairs "
                                "WHERE issue_id=? AND repair_key=?",
                                (token["issue_id"], expected_key),
                            ).fetchone()
                        if paid:
                            repair_key = expected_key
                            paid_worker = assignment["run_id"]
                            break
        budget = (
            shared.budget(token["issue_id"])
            if paid
            else shared.repair(token, repair_key, "Planning correction " + revision_id)
        )
        custody_evidence = _retained_evidence(store, spec, revision_id)
        context = {
            "revision_id": revision_id,
            "phase": "investigating",
            "owner": token,
            "sequence": 1 + len([key for key in values if key.startswith(PREFIX + "begin:")]),
            "custody_evidence": custody_evidence,
            "old_plan": deepcopy(record["manifest"]["plan"]),
            "old_identity": identity,
            "record": record,
            "diagnostic": deepcopy(diagnostic),
            "request": deepcopy(public["request"]) if public else None,
            "repair_key": repair_key,
            "paid_worker_run_id": paid_worker,
            "budget": budget,
            "candidate_id": candidate["id"],
        }
        shared.revision_checkpoint(token, PREFIX + "begin:" + revision_id, context)
        return deepcopy(context)


def _normalized_chunk(chunk):
    value = deepcopy(chunk)
    value["expected_paths"] = value.pop("allowed_paths", value.get("expected_paths", []))
    value.pop("workstream_id", None)
    value.pop("issue_number", None)
    value.setdefault("gates", [])
    return value


def _admit_plan(spec, old, proposed, identity):
    from .delivery_github_contract import ordered_chunks
    from .delivery_plan_model import validate_plan
    from .delivery_source_scope import require_authorized

    plan = validate_plan(proposed)
    if plan.get("version") != 2:
        raise ValueError("a new plan adoption requires the explicit v2 plan contract")
    if plan["scope"] != old["scope"] or plan["acceptance"] != old["acceptance"]:
        raise ValueError("plan correction cannot weaken or replace feature outcome/acceptance")
    final = {(gate["stage"], gate["recipe_id"]): gate for gate in plan["final_gates"]}
    for gate in old.get("final_gates", []):
        current = final.get((gate["stage"], gate["recipe_id"]))
        if (
            current is None
            or not set(gate["selectors"]) <= set(current["selectors"])
            or (not gate["selectors"] and current["selectors"])
        ):
            raise ValueError("plan correction cannot weaken accepted final gate obligations")
    old_streams = {stream["id"]: stream for stream in old["workstreams"]}
    streams = {stream["id"]: stream for stream in plan["workstreams"]}
    if streams.keys() != old_streams.keys():
        raise ValueError("plan correction cannot replace stable workstream identities")
    for ident, stream in streams.items():
        previous = old_streams[ident]
        binding = identity["workstream_issues"].get(ident)
        number = binding["number"] if binding else previous["issue_number"]
        if stream["issue_number"] != number or stream["acceptance"] != previous["acceptance"]:
            raise ValueError("plan correction changed child identity or workstream acceptance")
        if not {item["id"] for item in previous["chunks"]} <= {
            item["id"] for item in stream["chunks"]
        }:
            raise ValueError("plan correction cannot replace stable chunk ownership")
    before = {item["id"]: item for item in ordered_chunks(old)}
    after = {item["id"]: item for item in ordered_chunks(plan)}
    for key, item in after.items():
        if key in before and item["acceptance"] != before[key]["acceptance"]:
            raise ValueError("plan correction cannot weaken required chunk acceptance")
        require_authorized(spec["policy"], item["expected_paths"])
    from .delivery_feature_gates import validate_chunk_gates

    validate_chunk_gates(plan, spec)
    added = set(after) - set(before)
    changed = added | {
        key for key in before if _normalized_chunk(before[key]) != _normalized_chunk(after[key])
    }
    implementation = added | {
        key
        for key in before
        if any(
            _normalized_chunk(before[key])[field] != _normalized_chunk(after[key])[field]
            for field in ("scope", "steps", "expected_paths", "depends_on")
        )
    }
    if old.get("final_gates", []) != plan["final_gates"]:
        changed.add(ordered_chunks(plan)[-1]["id"])
    while True:
        enlarged = changed | {
            key for key, item in after.items() if set(item["depends_on"]) & changed
        }
        if enlarged == changed:
            break
        changed = enlarged
    # An implementation correction changes the content consumed by dependents.
    # Gate-only corrections keep their implementation/build evidence; source,
    # decomposition and dependency corrections invalidate its full closure.
    while True:
        enlarged = implementation | {
            key for key, item in after.items() if set(item["depends_on"]) & implementation
        }
        if enlarged == implementation:
            break
        implementation = enlarged
    if not changed and digest(old) == digest(plan):
        raise ValueError("plan proposal makes no progressing correction")
    return plan, sorted(changed), sorted(implementation)


def _role_context(store, spec, context, proposal=None):
    diagnostic = proposal["diagnostic"] if proposal else context["diagnostic"]
    references = (
        diagnostic["evidence"] if diagnostic else (context.get("request") or {}).get("evidence", [])
    )
    references = list(
        {
            item["path"]: item
            for item in (
                [*references, *context.get("custody_evidence", [])]
                if diagnostic
                else [*context.get("custody_evidence", []), *references]
            )
        }.values()
    )[:16]
    return {
        **deepcopy(context),
        "phase": "proposed" if proposal else "investigating",
        "namespace": "plan-revisions/" + context["revision_id"],
        "diagnostic": deepcopy(diagnostic),
        "proposed_plan_sha256": proposal["proposal_digest"] if proposal else None,
        "trusted_evidence": _evidence(store, spec, references),
    }


def _unreviewed_rejection(store, spec, context, rejected, values):
    """Prove an earlier closed review failed before launch, without reopening it.

    A missing session or caller reason cannot establish this exception. The
    supervisor's typed terminal result, exact historical requests and sealed
    public closure are all required; any missing or changed proof keeps the veto.
    """
    from .delivery_feature_revision_roles import revision_comparison
    from .delivery_resources import read_private
    from .supervisor import DeliverySupervisor

    try:
        revision_id = rejected["revision_id"]
        previous = values[PREFIX + "begin:" + revision_id]
        proposal = values[PREFIX + "proposal:" + revision_id]
        owner = previous["owner"]
        public = spec.get("feature_plan_revision_request")
        if (
            not public
            or values.get(PREFIX + "request:" + context["revision_id"]) != public
            or public["revision_id"] != context["revision_id"]
            or owner["generation"] >= context["owner"]["generation"]
            or previous["revision_id"] != revision_id
            or previous["phase"] != "investigating"
            or rejected["phase"] != "rejected"
            or _terminal(values, revision_id) != rejected
            or previous["old_identity"] != context["old_identity"]
            or previous["old_plan"] != context["old_plan"]
            or previous["candidate_id"] != context["candidate_id"]
            or proposal["revision_id"] != revision_id
            or proposal["phase"] != "proposed"
            or proposal["old_identity"] != previous["old_identity"]
            or proposal["proposal_digest"] != rejected["proposal_digest"]
            or digest(proposal["plan"]) != proposal["proposal_digest"]
            or proposal["repair_key"] != rejected["repair_key"]
            or proposal["repair_key"] != previous["repair_key"]
            or context["repair_key"] == previous["repair_key"]
        ):
            return None
        authenticate_adopted_plan_custody(store, spec, {"owner": owner})
        # Resolve this old run's immutable prepared authority, never the current
        # successor's input or a spec supplied by the saved role request.
        historical = store.effective_spec(owner["run_id"])
        with store._connect() as db:
            row = db.execute(
                "SELECT * FROM delivery_runs WHERE run_id=?", (owner["run_id"],)
            ).fetchone()
        if (
            not row
            or row["outcome"] not in {"blocked", "cancelled"}
            or row["cleanup"] != "confirmed"
            or row["recovery_json"] is not None
            or historical.get("provider") != "codex"
            or historical["policy"].get("execution_backend") != "native-macos"
            or not historical.get("preparation")
            or historical["feature_delivery"]["owner"] != owner
            or owner["store_path"] != str(store.config.tracking_db)
            or not store.owns_execution(historical)
            or row["request_digest"] != historical["request_digest"]
            or _assert_identity(previous["record"], previous["old_identity"])
            != context["old_identity"]
        ):
            return None
        closure_key, admission = next(
            (key, item)
            for key, item in values.items()
            if key.startswith(PREFIX + "request:")
            and item["owner"] == owner
            and item["predecessor_run_id"] == owner["run_id"]
            and item["old_identity"] == previous["old_identity"]
            and item["old_plan"] == previous["old_plan"]
        )
        closure = admission["predecessor_closure"]
        if (
            closure["workflow_id"] != store.active_workflow_id(owner["run_id"])
            or closure["request_digest"] != historical["request_digest"]
            or not isinstance(closure["execution_run_id"], str)
            or not closure["execution_run_id"]
        ):
            return None
        closed_at = datetime.fromisoformat(closure["closed_at"])
        candidate = json.loads(row["candidate_json"])
        if candidate["id"] != previous["candidate_id"] or closed_at.tzinfo is None:
            return None
        shared = _registry(spec)
        with shared.connect() as db:
            paid = db.execute(
                "SELECT generation FROM execution_repairs WHERE issue_id=? AND repair_key=?",
                (owner["issue_id"], previous["repair_key"]),
            ).fetchone()
            for key in (
                PREFIX + "begin:" + revision_id, PREFIX + "proposal:" + revision_id,
                PREFIX + "rejected:" + revision_id, closure_key,
                PREFIX + "request:" + context["revision_id"],
            ):
                checkpoint = db.execute(
                    "SELECT content_digest,content_json FROM execution_checkpoints "
                    "WHERE issue_id=? AND checkpoint_key=?", (owner["issue_id"], key),
                ).fetchone()
                if (not checkpoint or checkpoint["content_digest"] != digest(values[key])
                        or json.loads(checkpoint["content_json"]) != values[key]):
                    return None
        if not paid or paid["generation"] != owner["generation"]:
            return None
        proof = {}
        for role in ("intake", "review"):
            expected_context = _role_context(
                store, historical, previous, proposal if role == "review" else None
            )
            if role == "review":
                expected_context["proposed_plan"] = proposal["plan"]
            expected = {
                "spec": historical, "role": role, "iteration": 0,
                "candidate": candidate, "resume_session": None,
                "revision_context": expected_context,
            }
            key = DeliverySupervisor._job_key(expected)
            with store._connect() as db:
                attempt = db.execute(
                    "SELECT * FROM delivery_attempts WHERE run_id=? AND job_key=?",
                    (owner["run_id"], key),
                ).fetchone()
            folder = Path(historical["state_dir"]) / "attempts" / key
            expected.update(
                findings=[], native_authorized=True, role_evidence_key=key,
                result_path=str(folder / "result.json"), start_path=str(folder / "start.json"),
                workspace=(historical["checkout"] if role == "intake" else
                           str(Path(historical["state_dir"]) / "gates" / "0" / "review")),
            )
            request_path = folder / "request.json"
            if request_path.resolve(strict=True) != request_path:
                return None
            request = read_private(request_path)
            if role == "review":
                relative, comparison = revision_comparison(expected)
                comparison_path = Path(historical["state_dir"]) / relative
                diff = request.get("review_diff") or {}
                if comparison_path.resolve(strict=True) != comparison_path:
                    return None
                encoded = (json.dumps(comparison, sort_keys=True, indent=2) + "\n").encode()
                fd = os.open(comparison_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
                with os.fdopen(fd, "rb") as stream:
                    info = os.fstat(stream.fileno())
                    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                            or stream.read(len(encoded) + 1) != encoded):
                        return None
                expected["review_diff"] = {
                    "kind": "plan_revision", "path": str(comparison_path),
                    "sha256": hashlib.sha256(encoded).hexdigest(),
                    "base_sha": historical["base_sha"], "head": candidate["head"],
                    # The original producer omitted this outer field. Its inner
                    # receipt already binds the candidate; no other omission passes.
                    **({"candidate_id": candidate["id"]} if "candidate_id" in diff else {}),
                }
            result = json.loads(attempt["result_json"] or "null") if attempt else None
            if (
                request != expected
                or DeliverySupervisor._job_key(request) != key
                or not attempt
                or attempt["role"] != role
                or attempt["iteration"] != 0
                or attempt["candidate_id"] != candidate["id"]
                or attempt["result_path"] != str(folder / "result.json")
                or attempt["state"] != "finished"
                or attempt["cleanup"] != "confirmed"
                or not result
                or result.get("cleanup") != "confirmed"
                or datetime.fromisoformat(attempt["finished_at"]) > closed_at
            ):
                return None
            if role == "intake":
                if (
                    result.get("status") != "plan"
                    or result.get("plan") != proposal["plan"]
                    or result.get("diagnostic") != proposal["diagnostic"]
                    or not attempt["session_id"]
                    or result.get("session_id") != attempt["session_id"]
                ):
                    return None
            elif (
                result.get("status") != "blocked"
                or result.get("finish_reason") != "prelaunch"
                or any(
                    attempt[name] is not None for name in ("pid", "process_identity", "session_id")
                )
                or result.get("session_id") is not None
                or result.get("usage") is not None
                or any(os.path.lexists(folder / name) for name in (
                    "native-process.json", "launch.json", "ready.json", "start.json",
                    "result.json", "cancel",
                ))
            ):
                return None
            else:
                proof = {
                    "revision_id": revision_id, "review_job_key": key,
                    "review_request_digest": digest(request),
                    "review_result_digest": digest(result), "predecessor_closure": closure,
                }
        return proof
    except (KeyError, TypeError, ValueError, OSError, StopIteration):
        return None


def record_proposal(store, spec, revision_id, proposed_plan, *, diagnostic=None):
    context, values, terminal = _context(spec, revision_id)
    if terminal:
        raise OwnershipConflict("plan revision attempt is already closed")
    selected = context["diagnostic"] or diagnostic
    selected = validate_diagnostic(
        store,
        spec,
        selected,
        identity=context["old_identity"],
        candidate_id=context["candidate_id"],
    )
    if context["diagnostic"] and diagnostic is not None and diagnostic != context["diagnostic"]:
        raise OwnershipConflict("proposal replaced its sealed planning diagnostic")
    plan, affected, implementation = _admit_plan(
        spec, context["old_plan"], proposed_plan, context["old_identity"]
    )
    _preserve_started_custody(spec, context["old_plan"], plan, context["record"], values)
    if selected["chunk_id"] not in affected:
        raise ValueError("proposal does not correct the evidenced affected chunk")
    rejected = [value for key, value in values.items() if key.startswith(PREFIX + "rejected:")]
    unreviewed = []
    for value in rejected:
        if (value.get("proposal_digest") == digest(plan)
                and value.get("old_identity") == context["old_identity"]):
            proof = _unreviewed_rejection(store, spec, context, value, values)
            if not proof:
                raise ValueError("nonprogressing repeated proposal is already rejected")
            unreviewed.append(proof)
    receipt = {
        "revision_id": revision_id,
        "phase": "proposed",
        "plan": plan,
        "proposal_digest": digest(plan),
        "diagnostic": selected,
        "affected_chunks": affected,
        "implementation_chunks": implementation,
        "old_identity": context["old_identity"],
        "repair_key": context["repair_key"],
        **({"unreviewed_predecessors": unreviewed} if unreviewed else {}),
    }
    shared = _registry(spec)
    with shared.serialized(context["owner"]["issue_id"]):
        with shared.connect() as db:
            shared.require_settlement(db, context["owner"])
            shared._settled(db, context["owner"]["issue_id"])
        if adopted_plan_identity(spec, shared) != context["old_identity"]:
            raise OwnershipConflict("plan changed before proposal admission")
        shared.revision_checkpoint(context["owner"], PREFIX + "proposal:" + revision_id, receipt)
    return deepcopy(receipt)


def reject_revision(store, spec, revision_id, reason):
    context, values, terminal = _context(spec, revision_id)
    if terminal:
        return deepcopy(terminal)
    _text(reason, "revision rejection")
    shared = _registry(spec)
    with shared.connect() as db:
        shared.require_settlement(db, context["owner"])
        pending = db.execute(
            "SELECT 1 FROM execution_effects WHERE issue_id=? AND state='pending'",
            (context["owner"]["issue_id"],),
        ).fetchone()
    if pending:
        raise OwnershipConflict("revision cannot release custody with unsettled external effects")
    proposal = values.get(PREFIX + "proposal:" + revision_id, {})
    receipt = {
        "revision_id": revision_id,
        "phase": "rejected",
        "reason": reason,
        "old_identity": context["old_identity"],
        "proposal_digest": proposal.get("proposal_digest"),
        "repair_key": context["repair_key"],
        "budget": shared.budget(context["owner"]["issue_id"]),
    }
    shared.revision_checkpoint(context["owner"], PREFIX + "rejected:" + revision_id, receipt)
    return receipt


def _review_authorized(store, spec, context, proposal, review):
    if (
        not isinstance(review, dict)
        or review.get("status") != "pass"
        or review.get("reviewed_plan_sha256") != proposal["proposal_digest"]
        or review.get("candidate_id") != context["candidate_id"]
        or review.get("cleanup") != "confirmed"
        or review.get("findings")
    ):
        raise ValueError("plan adoption requires independent passing review of the exact proposal")
    if spec["provider"] != "fake" and (
        not review.get("session_id")
        or not review.get("proposal_session_id")
        or review["session_id"] == review["proposal_session_id"]
    ):
        raise ValueError("native plan review must use an independent recorded session")
    if spec["provider"] != "fake":
        from .supervisor import DeliverySupervisor

        candidate = _candidate(store, spec)
        sessions = {"intake": review["proposal_session_id"], "review": review["session_id"]}
        for role, session in sessions.items():
            request = {
                "spec": spec,
                "role": role,
                "iteration": 0,
                "candidate": candidate,
                "resume_session": None,
                "revision_context": {
                    "revision_id": context["revision_id"],
                    **({"proposed_plan": proposal["plan"]} if role == "review" else {}),
                },
            }
            key = DeliverySupervisor._job_key(request)
            with store._connect() as db:
                saved_role = db.execute(
                    "SELECT * FROM delivery_attempts WHERE run_id=? AND job_key=?",
                    (spec["run_id"], key),
                ).fetchone()
            result = json.loads(saved_role["result_json"] or "null") if saved_role else None
            if (
                not saved_role
                or saved_role["state"] != "finished"
                or saved_role["cleanup"] != "confirmed"
                or saved_role["session_id"] != session
                or not result
                or result.get("session_id") != session
                or (
                    role == "intake"
                    and (
                        result.get("status") != "plan"
                        or digest(result.get("plan")) != proposal["proposal_digest"]
                    )
                )
                or (
                    role == "review"
                    and (
                        result.get("status") != "pass"
                        or result.get("reviewed_plan_sha256") != proposal["proposal_digest"]
                        or result.get("findings")
                    )
                )
            ):
                raise ValueError("plan review lacks its exact independent durable role receipts")
    if spec.get("plan_approval") != "automatic":
        authorization = review.get("authorization")
        if not isinstance(authorization, dict):
            raise ValueError("original plan policy requires a durable revision approval")
        command = authorization.get("command")
        if not isinstance(command, dict):
            raise ValueError("revision approval requires its exact accepted decision command")
        with store._connect() as db:
            saved = db.execute(
                "SELECT * FROM delivery_mutations WHERE command_id=? AND run_id=?",
                (command.get("command_id"), spec["run_id"]),
            ).fetchone()
            row = db.execute(
                "SELECT candidate_revision FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
            ).fetchone()
            decisions = [
                json.loads(item[0]).get("decision")
                for item in db.execute(
                    "SELECT payload_json FROM delivery_events WHERE run_id=? "
                    "AND type='feature_revision_approval_pending'",
                    (spec["run_id"],),
                )
            ]
        decision = next(
            (
                item
                for item in decisions
                if item
                and item.get("id") == command.get("decision_id")
                and item.get("revision") == command.get("decision_revision")
            ),
            None,
        )
        if (
            not saved
            or saved["kind"] != "decision"
            or saved["state"] == "rejected"
            or saved["request_digest"] != digest(command)
            or command.get("answer") != "proceed"
            or authorization.get("proposed_plan_sha256") != proposal["proposal_digest"]
            or command.get("decision_id")
            != spec["run_id"] + ":plan-revision:" + context["revision_id"]
            or saved["decision_id"] != command.get("decision_id")
            or saved["decision_revision"] != command.get("decision_revision")
            or not decision
            or decision.get("kind") != "plan_revision"
            or decision.get("plan_digest") != proposal["proposal_digest"]
            or decision.get("id") != command.get("decision_id")
            or decision.get("revision") != command.get("decision_revision")
            or decision.get("candidate_revision") != command.get("candidate_revision")
            or not row
            or command.get("candidate_revision") != row[0]
        ):
            raise ValueError("revision approval is not an authenticated exact proposal decision")


def adopt_revision(store, spec, revision_id, proposed_plan, review, gh=None):
    """Publish exact remote revision first, then append its local adoption receipt."""
    from .delivery_github_contract import GitHubDelivery

    context, values, terminal = _context(spec, revision_id)
    if terminal:
        if terminal["phase"] != "adopted" or digest(proposed_plan) != digest(terminal["plan"]):
            raise OwnershipConflict("revision attempt already ended with a different result")
        revised = effective_spec(store, spec)
        _adoption_event(store, revised, terminal)
        return {
            "state": "adopted",
            "spec": revised,
            "plan_identity": terminal["identity"],
            "affected_chunks": terminal["affected_chunks"],
            "checkpoints": revision_checkpoints(revised, _values(revised)),
            "budget": _registry(spec).budget(context["owner"]["issue_id"]),
        }
    proposal = values.get(PREFIX + "proposal:" + revision_id)
    if not proposal or proposal["proposal_digest"] != digest(proposed_plan):
        raise OwnershipConflict("adoption differs from its sealed correction proposal")
    _review_authorized(store, spec, context, proposal, review)
    _evidence(store, spec, proposal["diagnostic"]["evidence"])
    shared = _registry(spec)
    token = context["owner"]
    with shared.connect() as db:
        shared.require_settlement(db, token)
        _settled_revision(db, shared, token, revision_id)
    if adopted_plan_identity(spec, shared) != context["old_identity"]:
        raise OwnershipConflict("plan adoption is stale")
    gh = gh or GitHubDelivery()
    issue = spec["feature_delivery"]["snapshot"]["issue"]
    # The publisher owns the effect lock and journal; never hold a second flock.
    record = gh.publish_plan_revision(
        issue, context["record"], proposal["plan"], shared, token, operation_id=revision_id
    )
    expected = {
        **context["old_identity"],
        "plan_revision": context["old_identity"]["plan_revision"] + 1,
        "plan_digest": proposal["proposal_digest"],
    }
    identity = _assert_identity(record, expected)
    if not _same_publication(
        record["manifest"]["publication"], context["record"]["manifest"]["publication"]
    ):
        raise OwnershipConflict("plan adoption changed retained stack publication identities")
    revised = deepcopy(spec)
    revised.pop("feature_plan_revision_request", None)
    revised["accepted_plan"] = canonical_json(proposal["plan"])
    revised["feature_plan_revision"] = {
        key: identity[key] for key in ("plan_revision", "plan_digest")
    }
    links = _child_links(record)
    receipt = {
        "revision_id": revision_id,
        "phase": "adopted",
        "owner": token,
        "old_identity": context["old_identity"],
        "identity": identity,
        "plan": proposal["plan"],
        "record": record,
        "diagnostic": proposal["diagnostic"],
        "affected_chunks": proposal["affected_chunks"],
        "implementation_chunks": proposal["implementation_chunks"],
        "repair_key": context["repair_key"],
        "diagnostic_worker_run_id": context.get("paid_worker_run_id")
        or _diagnostic_worker(store, spec, proposal["diagnostic"]),
        "budget": shared.budget(token["issue_id"]),
        "child_plan_links": links,
        "reason": proposal["diagnostic"]["detail"],
    }
    shared.adopt_plan_revision(token, receipt, revised)
    _adoption_event(store, revised, receipt)
    return {
        "state": "adopted",
        "spec": revised,
        "plan_identity": identity,
        "affected_chunks": proposal["affected_chunks"],
        "checkpoints": revision_checkpoints(revised, _values(revised)),
        "budget": receipt["budget"],
    }


def authenticate_revision_role(store, request):
    """Authenticate configured proposal/review work before any native claim or write."""
    spec = request.get("spec", {})
    supplied = request.get("revision_context")
    if (
        not isinstance(supplied, dict)
        or request.get("role") not in {"intake", "review"}
        or request.get("iteration") != 0
        or not spec.get("feature_delivery")
        or spec.get("feature_worker")
        or store.effective_spec(spec["run_id"]) != spec
    ):
        raise OwnershipConflict("revision role has no exact coordinator execution input")
    context, values, terminal = _context(spec, supplied.get("revision_id"))
    if terminal or adopted_plan_identity(spec) != context["old_identity"]:
        raise OwnershipConflict("revision role belongs to a superseded or closed proposal")
    for key in ("old_plan", "old_identity", "repair_key", "candidate_id"):
        if key in supplied and supplied[key] != context[key]:
            raise OwnershipConflict("revision role changed its sealed investigation context")
    shared = _registry(spec)
    with shared.connect() as db:
        shared.require_settlement(db, context["owner"])
        paid = db.execute(
            "SELECT 1 FROM execution_repairs WHERE issue_id=? AND repair_key=?",
            (context["owner"]["issue_id"], context["repair_key"]),
        ).fetchone()
    if (
        not paid
        or request.get("candidate") != _candidate(store, spec)
        or request["candidate"]["id"] != context["candidate_id"]
    ):
        raise OwnershipConflict("revision role has no matching repair debit or candidate")
    # Intake replay must retain its original begin context after the proposal
    # checkpoint was persisted but before Temporal acknowledged its result.
    proposal = (
        values.get(PREFIX + "proposal:" + context["revision_id"])
        if request["role"] == "review"
        else None
    )
    if request["role"] == "review" and (
        not proposal or digest(supplied.get("proposed_plan")) != proposal["proposal_digest"]
    ):
        raise OwnershipConflict("revision review changed its sealed proposal")
    return _role_context(store, spec, context, proposal)


def revision_checkpoints(spec, values):
    """Expose unaffected proof plus explicitly revised namespaces, keeping source history."""
    result = dict(values)
    receipts = sorted(
        (value for key, value in values.items() if key.startswith(PREFIX + "adopted:")),
        key=lambda value: value["identity"]["plan_revision"],
    )
    for receipt in receipts:
        affected = set(receipt["affected_chunks"])
        implementation = set(receipt["implementation_chunks"])
        for key in list(result):
            alias = key.split(":", 2)[-1] if key.startswith("pass:") else key
            if alias.startswith("verified:") and alias.removeprefix("verified:") in affected:
                result.pop(key)
            elif alias.startswith("build:") and alias.removeprefix("build:") in implementation:
                result.pop(key)
        prefix = "revision:" + str(receipt["identity"]["plan_revision"]) + ":"
        current_pass = values.get("integration-pass")
        pass_prefix = "pass:" + str(current_pass["number"]) + ":" if current_pass else None
        for key, value in values.items():
            if not key.startswith(prefix):
                continue
            alias = key[len(prefix) :]
            if alias.startswith("pass:"):
                if not pass_prefix or not alias.startswith(pass_prefix):
                    continue
                alias = alias[len(pass_prefix) :]
            result[alias] = value
    return result


def revision_checkpoint_key(spec, key):
    receipt = _latest(_values(spec))
    if not receipt:
        return key
    alias = key.split(":", 2)[-1] if key.startswith("pass:") else key
    affected = receipt["affected_chunks"]
    if (
        (alias.startswith("verified:") and alias.removeprefix("verified:") in affected)
        or (
            alias.startswith("build:")
            and alias.removeprefix("build:") in receipt["implementation_chunks"]
        )
        or (alias.startswith("assignment:") and alias.split(":")[1] in affected)
    ):
        return "revision:" + str(receipt["identity"]["plan_revision"]) + ":" + key
    return key


def local_revision_readback(spec, *, expected_revision=None, phase=None, cleanup=None):
    if not spec.get("feature_delivery") or spec.get("feature_worker"):
        return None
    from .delivery_feature_execution import plan_for
    from .delivery_github_contract import ordered_chunks
    from .delivery_source_scope import authority

    values = _values(spec)
    receipt = _latest(values)
    attempts = [value for key, value in values.items() if key.startswith(PREFIX + "begin:")]
    current = (
        max(attempts, key=lambda item: item.get("sequence", 0))
        if attempts
        else spec.get("feature_plan_revision_request")
    )
    terminal = _terminal(values, current["revision_id"]) if current else None
    proposal = values.get(PREFIX + "proposal:" + current["revision_id"]) if current else None
    selected = terminal or proposal or current or {}
    diagnostic = selected.get("diagnostic") or (current or {}).get("diagnostic") or {}
    plan = receipt["plan"] if receipt else (plan_for(spec) if spec.get("accepted_plan") else None)
    shared = _registry(spec)
    token = spec["feature_delivery"]["owner"]
    budget = shared.budget(token["issue_id"])
    reason = None
    if not plan:
        reason = "feature has no accepted GitHub plan yet"
    elif phase not in {"blocked", "cancelled"}:
        reason = "feature is not at a stopped checkpoint"
    elif cleanup != "confirmed":
        reason = "feature cleanup is unconfirmed"
    elif budget["used"] >= budget["maximum"]:
        reason = "feature product repair limit exhausted"
    with shared.connect() as db:
        current_owner = shared.current(token["issue_id"])
        if not reason and (
            not current_owner
            or shared.token(current_owner) != token
            or current_owner["state"] != "stopped"
        ):
            reason = "feature coordinator is not its stopped execution owner"
        if not reason:
            try:
                shared._settled(db, token["issue_id"])
            except OwnershipConflict as exc:
                reason = str(exc)
        if not reason and shared._pending_revision(db, token["issue_id"]):
            reason = "recorded revision remains in custody"
    return {
        "plan_identity": adopted_plan_identity(spec),
        "phase": selected.get("phase", "current" if plan else "unplanned"),
        "revision_id": selected.get("revision_id"),
        "reason": selected.get("reason")
        or diagnostic.get("detail")
        or ((current or {}).get("request") or {}).get("reason"),
        "evidence": diagnostic.get("evidence", [])
        or ((current or {}).get("request") or {}).get("evidence", []),
        "affected_chunks": selected.get("affected_chunks", []),
        "repair_budget": budget,
        "expected_revision": expected_revision,
        "child_plan_links": (receipt or {}).get("child_plan_links", {})
        or _child_links(spec["feature_delivery"]["snapshot"].get("delivery") or {}),
        "authority": authority(spec["policy"]),
        "expected_paths": {
            chunk["id"]: chunk.get("expected_paths", chunk.get("allowed_paths", []))
            for chunk in (ordered_chunks(plan) if plan else [])
        },
        "can_revise": reason is None,
        "reason_ineligible": reason,
    }


def revision_readback(store, spec):
    if not spec.get("feature_delivery") or spec.get("feature_worker"):
        return None
    with store._connect() as db:
        row = db.execute(
            "SELECT revision,phase,cleanup FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
    result = local_revision_readback(spec, expected_revision=row[0], phase=row[1], cleanup=row[2])
    return result


current_plan_identity = adopted_plan_identity


def authenticate_initial_plan_input(store, spec):
    """Replay only the exact original v2 input after its initial normalization."""
    current = store.effective_spec(spec["run_id"])
    if current == spec:
        return spec
    if spec.get("feature_plan_version") != 2 or spec.get("feature_plan_revision"):
        raise OwnershipConflict("initial plan replay has no original v2 input authority")
    shared = _registry(spec)
    receipt = _latest(_values(spec, shared))
    if (
        not receipt
        or receipt["identity"]["plan_revision"] != 1
        or not receipt["revision_id"].startswith("initial-")
        or receipt.get("input_spec_digest") != digest(spec)
        or receipt["owner"] != spec["feature_delivery"]["owner"]
        or store._effective_spec(spec["run_id"]) != spec
    ):
        raise OwnershipConflict("initial plan replay changed its canonical admitted input")
    with shared.connect() as db:
        shared.require_settlement(db, receipt["owner"])
    return current


def record_initial_plan_adoption(spec, record, shared=None):
    """Resolve initial v2 child IDs without changing acceptance or consuming a repair."""
    original = json.loads(spec["accepted_plan"])
    resolved = record["manifest"]["plan"]
    if original.get("version") != 2 or resolved.get("version") != 2:
        return None
    normalized = deepcopy(original)
    bindings = record["manifest"]["workstream_issues"]
    for stream in normalized["workstreams"]:
        binding = bindings.get(stream["id"])
        if not binding or (
            stream["issue_number"] is not None and stream["issue_number"] != binding["number"]
        ):
            raise OwnershipConflict("initial v2 plan has different resolved child identities")
        stream["issue_number"] = binding["number"]
    if normalized != resolved or record["manifest"].get("plan_revision") != 1:
        raise OwnershipConflict("initial normalization changed more than unresolved child numbers")
    shared = _registry(spec, shared)
    identity = {
        "plan_revision": 1,
        "plan_digest": digest(resolved),
        "comment_id": record["comment_id"],
        "comment_node_id": record["comment_node_id"],
        "workstream_issues": deepcopy(bindings),
    }
    revised = deepcopy(spec)
    revised["accepted_plan"] = canonical_json(resolved)
    revised["feature_plan_revision"] = {
        key: identity[key] for key in ("plan_revision", "plan_digest")
    }
    receipt = {
        "revision_id": "initial-" + identity["plan_digest"][:24],
        "phase": "adopted",
        "owner": spec["feature_delivery"]["owner"],
        "old_identity": identity,
        "identity": identity,
        "plan": resolved,
        "record": record,
        "affected_chunks": [],
        "implementation_chunks": [],
        "repair_key": None,
        "budget": shared.budget(receipt_issue_id(spec)),
        "child_plan_links": _child_links(record),
        "reason": "Initial GitHub child-number normalization",
        "input_spec_digest": digest(spec),
    }
    shared.initialize_plan_revision(spec["feature_delivery"]["owner"], receipt, revised)
    return receipt


def receipt_issue_id(spec):
    return spec["feature_delivery"]["owner"]["issue_id"]


def worker_revision_required(parent, child):
    receipt = _latest(_values(parent))
    if not receipt or receipt["identity"]["plan_revision"] <= 1:
        return False
    expected = {key: receipt["identity"][key] for key in ("plan_revision", "plan_digest")}
    return child.get("feature_plan_revision") != expected


def worker_revision_affected(parent, child, *, through_revision=None, implementation=False):
    previous = child.get("feature_plan_revision", {}).get("plan_revision", 1)
    chunk = child["feature_worker"]["chunk_id"]
    field = (
        "implementation_chunks"
        if implementation or child["feature_worker"]["kind"] == "build"
        else "affected_chunks"
    )
    return any(
        chunk in receipt[field]
        and receipt["identity"]["plan_revision"] > previous
        and (through_revision is None or receipt["identity"]["plan_revision"] <= through_revision)
        for key, receipt in _values(parent).items()
        if key.startswith(PREFIX + "adopted:")
    )


def resume_revision_worker(store, parent, child, row=None):
    from .delivery_feature_revision_recovery import resume_revision_worker as resume

    return resume(store, parent, child, row)


def _child_links(record):
    from .delivery_github_plans import child_plan_links

    return child_plan_links(record) if record.get("manifest") else {}


def _diagnostic_worker(store, spec, diagnostic):
    matches = {}
    for key, assignment in _values(spec).items():
        if (
            "assignment:" not in key
            or not isinstance(assignment, dict)
            or assignment.get("chunk_id") != diagnostic["chunk_id"]
            or assignment.get("store_path") != str(store.config.tracking_db)
        ):
            continue
        with store._connect() as db:
            row = db.execute(
                "SELECT candidate_json FROM delivery_runs WHERE run_id=?", (assignment["run_id"],)
            ).fetchone()
        if row and json.loads(row[0] or "{}").get("id") == diagnostic["candidate_id"]:
            matches[assignment["run_id"]] = assignment
    ordered = sorted(matches.values(), key=lambda item: item["kind"] != "chunk")
    return ordered[0]["run_id"] if ordered else None


def _same_publication(current, previous):
    if current.get("stack_id") != previous.get("stack_id"):
        return False
    before, after = previous.get("members", []), current.get("members", [])
    return len(before) == len(after) and all(
        all(member.get(key) == value for key, value in prior.items())
        for member, prior in zip(after, before, strict=True)
    )


def _settled_revision(db, shared, token, revision_id):
    if db.execute(
        "SELECT 1 FROM execution_workers WHERE issue_id=? AND state!='finished'",
        (token["issue_id"],),
    ).fetchone():
        raise OwnershipConflict("revision still has live or unknown workers")
    pending = list(
        db.execute(
            "SELECT kind,request_json FROM execution_effects WHERE issue_id=? AND state='pending'",
            (token["issue_id"],),
        )
    )
    if any(
        row["kind"] not in {"github_child_plan", "github_plan_revision"}
        or json.loads(row["request_json"]).get("operation_id") != revision_id
        for row in pending
    ):
        raise OwnershipConflict("revision has an unrelated unsettled external effect")


def _adoption_event(store, spec, receipt):
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        if db.execute(
            "SELECT 1 FROM delivery_events WHERE run_id=? AND type='feature_plan_revision_adopted' "
            "AND json_extract(payload_json,'$.revision_id')=?",
            (spec["run_id"], receipt["revision_id"]),
        ).fetchone():
            return
        db.execute("UPDATE delivery_runs SET revision=revision+1 WHERE run_id=?", (spec["run_id"],))
        row = db.execute(
            "SELECT revision FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        store._event(
            db,
            spec["run_id"],
            row[0],
            "feature_plan_revision_adopted",
            "GitHub plan correction adopted; original execution retained",
            {
                "revision_id": receipt["revision_id"],
                "plan_identity": receipt["identity"],
                "affected_chunks": receipt["affected_chunks"],
                "repair_budget": receipt["budget"],
            },
        )


def _bounded_fact(value, maximum=1024):
    encoded = canonical_json(value)
    if len(encoded) <= maximum:
        return value
    return {"sha256": digest(value), "excerpt": encoded[:maximum], "truncated": True}


def _retained_evidence(store, spec, revision_id):
    """Seal a small complete failure fact per child for trusted native intake."""
    from .delivery_metadata_recovery import _immutable
    from .delivery_resources import private_directory

    current_identity = adopted_plan_identity(spec)
    identity = {key: current_identity[key] for key in ("plan_revision", "plan_digest")}
    children = {}
    for key, assignment in _values(spec).items():
        if "assignment:" not in key or not isinstance(assignment, dict):
            continue
        run_id = assignment.get("run_id")
        if run_id in children or assignment.get("store_path") != str(store.config.tracking_db):
            continue
        with store._connect() as db:
            saved = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        if not saved:
            continue
        child = store.effective_spec(run_id)
        row = dict(saved)
        checks = json.loads(row["checks_json"] or "{}")
        failures = {
            stage: {
                field: value
                for field, value in result.items()
                if field
                in {
                    "state",
                    "candidate_id",
                    "error",
                    "reason",
                    "required_selectors",
                    "results",
                }
            }
            for stage, result in checks.items()
            if isinstance(result, dict) and result.get("state") in {"failed", "unknown"}
        }
        candidate = json.loads(row["candidate_json"] or "null")
        browser = child["policy"].get("browser_qa") or {}
        authority = {key: child["policy"].get(key) for key in ("allowed_paths", "source_scope")}
        facts = {
            "version": 1,
            "plan_identity": identity,
            "run_id": run_id,
            "chunk_id": assignment["chunk_id"],
            "kind": assignment["kind"],
            "request_digest": row["request_digest"],
            "original_spec_digest": digest(json.loads(row["request_json"])),
            "effective_spec_digest": digest(child),
            "iteration": row["iteration"],
            "phase": row["phase"],
            "cleanup": row["cleanup"],
            "error": (row["error"] or "")[:1000],
            "candidate_id": (candidate or {}).get("id"),
            "candidate": candidate,
            "publication": json.loads(row["pr_json"] or "null"),
            "accepted_chunk_sha256": digest(json.loads(child["accepted_plan"])),
            "file_authority_sha256": digest(authority),
            "expected_paths": child.get("expected_paths", authority.get("allowed_paths")),
            "browser_qa": {
                key: browser.get(key)
                for key in (
                    "id",
                    "argv",
                    "required_selectors",
                    "selector_project",
                )
            },
            "failed_checks": failures,
            "workflow_id": row["workflow_id"],
        }
        # Every sealed file fits the role's 8 KiB trusted excerpt. Keep exact
        # identities and failure status; large artifacts remain digest-bound
        # local evidence rather than displacing the useful failure facts.
        for maximum in (1024, 512, 256, 128):
            bounded = {
                **facts,
                **{
                    field: _bounded_fact(facts[field], maximum)
                    for field in (
                        "candidate",
                        "publication",
                        "expected_paths",
                        "browser_qa",
                        "failed_checks",
                    )
                },
            }
            if len((json.dumps(bounded, sort_keys=True, indent=2) + "\n").encode()) <= 8192:
                break
        else:
            raise ValueError("retained worker failure facts exceed the bounded native excerpt")
        children[run_id] = bounded
        if len(children) >= 16:
            break
    root = Path(spec["state_dir"]) / "plan-revisions" / revision_id
    private_directory(root)
    references = []
    ordered = sorted(
        children.values(),
        key=lambda child: (not bool(child["failed_checks"]), child["kind"] != "chunk"),
    )
    for child in ordered:
        path = root / ("retained-worker-" + digest(child["run_id"])[:24] + ".json")
        _immutable(path, child)
        references.append(
            {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        )
    if not references:
        path = root / "retained-worker-evidence.json"
        _immutable(path, {"version": 1, "plan_identity": identity, "workers": {}})
        references.append(
            {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
        )
    return references


def _preserve_started_custody(spec, old, new, record, values):
    from .delivery_github_contract import ordered_chunks

    before = {chunk["id"]: chunk for chunk in ordered_chunks(old)}
    after = {chunk["id"]: chunk for chunk in ordered_chunks(new)}
    started = {
        value["chunk_id"]
        for key, value in values.items()
        if "assignment:" in key and isinstance(value, dict) and value.get("chunk_id")
    }
    for key in started:
        if key not in after or after[key]["depends_on"] != before[key]["depends_on"]:
            raise OwnershipConflict("plan correction changed started chunk dependency custody")
    members = record["manifest"]["publication"]["members"]
    if [item["id"] for item in ordered_chunks(new)[: len(members)]] != [
        member["chunk_id"] for member in members
    ]:
        raise OwnershipConflict("plan correction reordered the exact published stack prefix")
