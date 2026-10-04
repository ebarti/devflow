"""One source-applicable investigation disposition, followed only by controller final gates.

This preserves the failed native assessment. It authorizes no native execution under
an old payload proof, and never enters an implementation or preparation route.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_gates_admission import _assessment_receipt, _native_result_bytes
from .delivery_metadata_recovery import _immutable
from .delivery_policy_recovery import _rows, work_binding
from .delivery_resources import _ancestors, private_directory, read_private
from .delivery_technical_integration import reference

KIND = "investigation_assessment_adjudication"
FIELDS = {
    "continuation_kind",
    "command_id",
    "expected_revision",
    "expected_iteration",
    "expected_candidate_id",
    "expected_pr_number",
    "expected_pr_head",
    "additional_iterations",
    "authority_path",
    "authority_sha256",
    "controller_path",
    "controller_sha256",
}
LIMITS = {
    "adjudication_commands": 1,
    "additional_iterations": 0,
    "feature_implementation_grants": 0,
    "provider_or_model_turns": 0,
    "native_preparation_generations": 0,
    "native_local_check_reruns": 0,
    "publication_or_git_source_changes": 0,
    "b3_successor_or_native_counter_resets": 0,
}
ACTIVITIES = {
    "delivery_adjudication_readback",
    "delivery_ci",
    "delivery_project",
    "delivery_finalize_resources",
    "delivery_terminal_tracker",
}


def _request(payload):
    if (
        not isinstance(payload, dict)
        or set(payload) != FIELDS
        or payload["continuation_kind"] != KIND
        or type(payload["additional_iterations"]) is not int
        or payload["additional_iterations"] != 0
        or type(payload["expected_iteration"]) is not int
        or payload["expected_iteration"] != 4
        or type(payload["expected_revision"]) is not int
        or payload["expected_revision"] < 1
        or type(payload["expected_pr_number"]) is not int
        or payload["expected_pr_number"] < 1
        or not isinstance(payload["command_id"], str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", payload["command_id"])
        or any(
            not isinstance(payload[k], str)
            or not re.fullmatch(r"[0-9a-f]{" + str(n) + "}", payload[k])
            for k, n in [
                ("expected_candidate_id", 64),
                ("expected_pr_head", 40),
                ("authority_sha256", 64),
                ("controller_sha256", 64),
            ]
        )
    ):
        raise ValueError("adjudication requires an explicit same-iteration zero-grant request")


def _bytes(path, sha256, *, private=True, limit=8 * 1024 * 1024):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path or ".." in path.parts:
        raise ValueError("adjudication evidence is aliased or noncanonical")
    _ancestors(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_size > limit
            or (private and stat.S_IMODE(before.st_mode) != 0o600)
            or (not private and before.st_mode & 0o022)
        ):
            raise ValueError("adjudication evidence custody or finite size changed")
        with os.fdopen(os.dup(fd), "rb") as stream:
            raw = stream.read(limit + 1)
        after = os.fstat(fd)
        if (
            len(raw) != before.st_size
            or hashlib.sha256(raw).hexdigest() != sha256
            or any(
                getattr(before, k) != getattr(after, k)
                for k in (
                    "st_dev",
                    "st_ino",
                    "st_mode",
                    "st_uid",
                    "st_nlink",
                    "st_size",
                    "st_mtime_ns",
                    "st_ctime_ns",
                )
            )
        ):
            raise ValueError("adjudication evidence hash or read custody changed")
        return raw
    finally:
        os.close(fd)


def _authority(payload, run_id):
    value = reference(payload["authority_path"], payload["authority_sha256"])
    if (
        value.get("kind") != "original_investigation_assessment_adjudication_owner_decision"
        or value.get("owner") != "root"
        or value.get("schema") != 1
        or value.get("run_id") != run_id
        or value.get("iteration") != 4
        or value.get("finite_limits") != LIMITS
        or value.get("required_initial_terminal_cleanup") != "confirmed"
        or value.get("raw_qa", {}).get("status") != "findings"
        or len(value.get("raw_qa", {}).get("findings", [])) != 3
        or value.get("accepted_disposition", {}).get("remaining_blocker_high") != 0
        or value.get("accepted_disposition", {}).get("accepted_baseline_medium") != 2
        or value.get("accepted_disposition", {}).get("raw_status_remains") != "findings"
        or value.get("historical_gate_applicability", {}).get("explicitly_authorized") is not True
        or value["historical_gate_applicability"].get("runtime_sha")
        != value.get("original_runtime_sha")
        or value["historical_gate_applicability"].get("source_unchanged_required") is not True
        or value["historical_gate_applicability"].get(
            "all_actual_receipt_and_artifact_hashes_required"
        )
        is not True
    ):
        raise ValueError("adjudication authority changes its finite investigation disposition")
    evidence = {k: reference(v["path"], v["sha256"]) for k, v in value["bindings"].items()}
    if set(evidence) != {
        "source_receipt_report",
        "independent_review23",
        "root_independent_reproduction",
        "accepted_investigation_outcome",
    }:
        raise ValueError("adjudication authority has incomplete independent evidence")
    raw = json.loads(_bytes(value["raw_qa"]["path"], value["raw_qa"]["sha256"]))
    reproduction = evidence["root_independent_reproduction"]
    report = evidence["source_receipt_report"]
    review = evidence["independent_review23"]
    candidate = value["candidate"]
    if (
        raw.get("status") != "findings"
        or raw.get("findings") != value["raw_qa"]["findings"]
        or raw.get("session_id") != value["raw_qa"]["session_id"]
        or reproduction.get("candidate") != candidate
        or reproduction.get("raw_assessment")
        != {k: value["raw_qa"][k] for k in ("path", "sha256", "status", "findings")}
        or reproduction.get("runtime_source") != value["original_runtime_sha"]
        or reproduction.get("independent_reproduction", {}).get("actual_receipt_hashes_exact_match")
        is not True
        or [d.get("state") for d in reproduction.get("dispositions", [])]
        != [
            "false_positive",
            "accepted_baseline_investigation_observation",
            "accepted_baseline_investigation_observation",
        ]
        or report.get("feature_candidate") != candidate
        or report.get("raw_status") != "findings"
        or report.get("raw_findings") != raw["findings"]
        or report.get("session_id") != raw["session_id"]
        or report.get("candidate_runtime") != value["original_runtime_sha"]
        or review.get("counts") != {"Blocker": 0, "High": 0, "Medium": 2, "Low": 0}
        or review.get("candidate") != candidate["id"]
        or review.get("feature_head") != candidate["head"]
        or review.get("feature_base") != candidate["base_sha"]
        or review.get("runtime") != value["original_runtime_sha"]
        or review.get("evidence", {}).get("raw_verify") != value["raw_qa"]["sha256"]
        or review["evidence"].get("verification_report")
        != value["bindings"]["source_receipt_report"]["sha256"]
        or review["evidence"].get("accepted_investigation_authority")
        != value["bindings"]["accepted_investigation_outcome"]["sha256"]
        or review.get("independent_fresh_artifact_rehash")
        != {
            "files": 311,
            "cases": 8,
            "requested_pdfs": 48,
            "requested_page_images": 132,
            "visual_groups": 22,
            "passed": True,
        }
    ):
        raise ValueError("raw QA or independent source/applicability classification changed")
    return value, evidence


def _controller(store, spec, payload):
    """Bind the reviewed installed controller; this does not authorize native execution."""
    value = reference(payload["controller_path"], payload["controller_sha256"])
    from .delivery_native_preparation import PACKAGE
    from .delivery_native_process import process_table
    from .payload import payload_digest

    source = Path(__file__).resolve().parents[3]
    config_raw = _bytes(spec["config_path"], value.get("config_sha256"))
    processes_path = store.config.state_root / "service-processes.json"
    processes = json.loads(_bytes(processes_path, value.get("service_manifest_sha256")))
    active = value.get("active_config", {})
    active_raw = _bytes(Path(active.get("path", "")), active.get("sha256"))
    frozen_policy = json.loads(config_raw)
    active_policy = json.loads(active_raw)
    # A user-authorized role effort change cannot rewrite a sealed run config.
    # Bind both configurations and require every non-role field to remain exact.
    if (active.get("path") != processes.get("config_path")
            or active.get("roles") != active_policy.get("roles")
            or {k: v for k, v in frozen_policy.items() if k != "roles"}
            != {k: v for k, v in active_policy.items() if k != "roles"}):
        raise ValueError("controller active policy changed non-role run authority")
    table = process_table()
    if (
        value.get("kind") != "root_installed_controller_final_tail_readback"
        or value.get("owner") != "root"
        or value.get("run_id") != spec["run_id"]
        or value.get("authority_sha256") != payload["authority_sha256"]
        or value.get("installed_source_root") != str(source)
        or value.get("source_revision") != _git(source, "rev-parse", "HEAD")
        or value.get("source_tree") != _git(source, "rev-parse", "HEAD^{tree}")
        or _git(source, "status", "--porcelain", "--untracked-files=all")
        or value.get("runtime_payload_sha256") != payload_digest(PACKAGE)
        or value.get("config_path") != spec["config_path"]
        or value.get("config_digest") != spec["config_digest"]
        or digest(json.loads(config_raw)) != spec["config_digest"]
        or value.get("published_tree") != value["source_tree"]
        or _git(source, "rev-parse", value.get("published_head", "") + "^{tree}")
        != value["source_tree"]
        or value.get("source_review") != "PASS"
        or value.get("required_ci") != "SUCCESS"
        or value.get("service_manifest_path") != str(processes_path)
        or active_policy.get("state_root") != str(store.config.state_root)
        or set(processes.get("processes", {})) != {"api", "worker", "temporal"}
        or any(
            table.get(p["pid"], {}).get("identity") != p["identity"]
            or table[p["pid"]]["stat"].startswith("Z")
            for p in processes["processes"].values()
        )
    ):
        raise ValueError("final controller source/config/publication/CI/service identity changed")
    return value


def _historical(store, spec, previous, authority):
    """Authenticate immutable old generations; never call a current execution verifier."""
    from .delivery_native_renewal import effective_spec as native_spec
    from .delivery_technical_continuation import _retained

    seal = _retained(store, previous)
    if (
        previous.get("kind") != "accepted_technical_successor"
        or previous.get("integration") is not None
        or previous.get("candidate", {}).get("id") != authority["candidate"]["id"]
        or previous.get("execution_spec") != spec
        or seal.get("resume_stage") != "review"
    ):
        raise ValueError("adjudication skipped its immediate consumed technical generation")
    value = native_spec(seal["proposed_spec"], previous, technical=True)
    receipt = read_private(Path(previous["native_preparation_renewal"]["path"]))
    if (
        value != spec
        or receipt.get("source_revision") != authority["original_runtime_sha"]
        or receipt.get("config_sha256")
        != hashlib.sha256(Path(spec["config_path"]).read_bytes()).hexdigest()
    ):
        raise ValueError("historical native source/payload generation changed")
    return previous["native_preparation_renewal"]


def _gates(spec, state, attempts, authority, evidence):
    candidate = authority["candidate"]
    canonical_candidate = {k: v for k, v in candidate.items() if k != "revision"}
    if (
        state.get("candidate") != canonical_candidate
        or state.get("candidate_revision") != candidate["revision"]
    ):
        raise ValueError("adjudication candidate/source revision changed")
    local = state.get("checks", {}).get("local", {})
    prepublish = state.get("checks", {}).get("prepublish", {})
    if (
        local.get("state") != "passed"
        or prepublish.get("state") != "passed"
        or local.get("source_unchanged") is not True
        or local.get("candidate_id") != candidate["id"]
        or len(local.get("results", [])) != 6
        or len(prepublish.get("results", [])) != 6
        or [r.get("id") for r in local["results"]] != [c["id"] for c in spec["policy"]["checks"]]
    ):
        raise ValueError("adjudication needs every completed original native gate")
    report = evidence["source_receipt_report"]
    reproduction = evidence["root_independent_reproduction"]
    evidence_hashes = {}
    for stage in (prepublish, local):
        for result in stage["results"]:
            if (
                result.get("passed") is not True
                or result.get("cleanup") != "confirmed"
                or result.get("process_cleanup") != "observed-native-confirmed"
                or result.get("exit_code") != 0
                or result.get("rejection_causes")
                or result.get("native_process", {}).get("monitoring_complete") is not True
            ):
                raise ValueError("adjudication cannot accept an incomplete or rejected native gate")
            _bytes(result["log"], result["log_sha256"])
            evidence_hashes[result["log"]] = result["log_sha256"]
    qa = [
        a
        for a in attempts
        if a["role"] == "verify"
        and a["iteration"] == 4
        and a.get("session_id") == authority["raw_qa"]["session_id"]
    ]
    review = [
        r
        for r in state["roles"]
        if r["role"] == "review" and r["iteration"] == 4 and r.get("status") == "pass"
    ]
    if len(qa) != 1 or len(review) != 1 or review[0].get("candidate") != canonical_candidate:
        raise ValueError("adjudication review/QA checkpoint is not the completed independent pair")
    review_attempt = [
        a
        for a in attempts
        if a["role"] == "review"
        and a["iteration"] == 4
        and a.get("session_id") == review[0].get("session_id")
    ]
    if len(review_attempt) != 1 or review[0].get("session_id") == qa[0]["session_id"]:
        raise ValueError("adjudication review receipt/session is not independent")
    review_raw = _native_result_bytes(spec, review_attempt[0])
    _assessment_receipt(spec, review_attempt[0], review_raw)
    if json.loads(review_raw).get("status") != "pass":
        raise ValueError("adjudication review raw receipt did not pass")
    raw = _native_result_bytes(spec, qa[0])
    _assessment_receipt(spec, qa[0], raw)
    if hashlib.sha256(raw).hexdigest() != authority["raw_qa"]["sha256"]:
        raise ValueError("adjudication raw assessment is a different native result")
    role = [
        r
        for r in state["roles"]
        if r.get("role") == "verify" and r.get("session_id") == qa[0]["session_id"]
    ]
    if (
        len(role) != 1
        or role[0].get("status") != "findings"
        or role[0].get("findings") != authority["raw_qa"]["findings"]
        or role[0].get("candidate") != canonical_candidate
        or state["checks"].get("qa", {}).get("state") != "failed"
        or state.get("findings", [])[-3:] != authority["raw_qa"]["findings"]
    ):
        raise ValueError("adjudication does not own exactly the classified raw findings")
    for binding in report["hashes"].values():
        if not Path(binding["original"]).is_relative_to(
            Path(spec["state_dir"]) / "attempts" / qa[0]["job_key"]
        ):
            raise ValueError("adjudication QA evidence left its original attempt")
        _bytes(binding["original"], binding["sha256"])
        evidence_hashes[binding["original"]] = binding["sha256"]
    producer = reproduction["producer"]
    _bytes(producer["path"], producer["sha256"], private=False)
    if (
        report["producer_module"] != producer
        or evidence["independent_review23"]["evidence"]["producer"] != producer["sha256"]
    ):
        raise ValueError("adjudication staging producer lineage changed")
    dependency = reproduction["actual_dependency_receipt"]
    receipt = json.loads(_bytes(dependency["path"], dependency["sha256"]))
    from .delivery_native_dependencies import frozen_pnpm_inputs

    manager, staged = frozen_pnpm_inputs(spec, Path(report["checkout"]))
    hashes = {name: hashlib.sha256(raw).hexdigest() for name, raw in staged.items()}
    if (
        manager != receipt["package_manager"]
        or hashes != receipt["input_hashes"]
        or hashes != dependency["recorded_input_hashes"]
        or evidence["independent_review23"]["evidence"]["receipt"] != dependency["sha256"]
    ):
        raise ValueError("adjudication staged dependency receipt semantics changed")
    artifacts = [r["artifacts"] for r in local["results"] if r.get("artifacts")]
    if len(artifacts) != 1 or artifacts[0]["count"] != 311:
        raise ValueError("adjudication lacks the complete fresh artifact inventory")
    binding = artifacts[0]
    manifest = json.loads(_bytes(binding["path"], binding["sha256"]))
    if (
        manifest.get("candidate_id") != candidate["id"]
        or binding["candidate_id"] != candidate["id"]
        or manifest.get("count") != 311
        or len(manifest.get("artifacts", [])) != 311
        or len({i["path"] for i in manifest["artifacts"]}) != 311
        or manifest.get("bytes") != sum(i["size"] for i in manifest["artifacts"])
    ):
        raise ValueError("adjudication artifact count/candidate changed")
    root = Path(binding["path"]).parent / "artifacts"
    for item in manifest["artifacts"]:
        file = Path(item["path"])
        if (
            not file.is_relative_to(root)
            or file.relative_to(root).as_posix() != item["relative_path"]
        ):
            raise ValueError("adjudication artifact left its owned manifest")
        raw = _bytes(file, item["sha256"], limit=50 * 1024 * 1024)
        if len(raw) != item["size"]:
            raise ValueError("adjudication artifact size changed")
    return {
        **evidence_hashes,
        binding["path"]: binding["sha256"],
        dependency["path"]: dependency["sha256"],
    }


def _snapshot(store, run_id, payload):
    authority, evidence = _authority(payload, run_id)
    spec = store.effective_spec(run_id)
    row, attempts, effects, claim = _rows(store, run_id)
    previous = json.loads(row["recovery_json"])
    controller = _controller(store, spec, payload)
    _historical(store, spec, previous, authority)
    closed = store._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
    state = closed["result"]
    if (
        spec["work_id"] != authority["work_id"]
        or spec["provider"] != "codex"
        or spec["terminal_tracker_version"] != 1
        or spec["resource_cleanup_version"] != 1
        or row["workflow_id"] != authority["original_technical_workflow_id"]
        or closed["workflow_id"] != row["workflow_id"]
        or closed["request_digest"] != spec["request_digest"]
        or closed.get("recovery_digest") != digest(previous)
        or state.get("run_id") != run_id
        or state.get("outcome") != "blocked"
        or state.get("phase") != "blocked"
        or state.get("cleanup") != "confirmed"
        or row["outcome"] != "blocked"
        or row["cleanup"] != "confirmed"
        or row["iteration"] != 4
        or row["protocol_revision"] != payload["expected_revision"]
        or state["revision"] != payload["expected_revision"]
        or state.get("iteration") != 4
        or claim is not None
        or authority["candidate"]["id"] != payload["expected_candidate_id"]
        or authority["candidate"]["head"] != payload["expected_pr_head"]
        or state.get("pull_request", {}).get("number") != payload["expected_pr_number"]
        or any(a["state"] != "finished" or a["cleanup"] == "unknown" for a in attempts)
        or any(e["state"] not in {"complete", "failed"} for e in effects)
    ):
        raise ValueError("adjudication lost its exact stopped raw QA checkpoint")
    with store._connect() as db:
        work_binding(store, spec, db)
        sequence = db.execute(
            "SELECT MAX(sequence) FROM delivery_events WHERE run_id=?", (run_id,)
        ).fetchone()[0]
        if sequence != authority["terminal_sequence"]:
            raise ValueError("adjudication terminal event sequence changed")
    from .delivery_repair import published_identity
    from .delivery_technical_continuation import _observe_resources, _quiescent

    _quiescent(store, spec)
    resources = _observe_resources(spec, unknown_allowed=False)
    published_identity(DeliveryBroker(store, spec), state["candidate"], state["pull_request"])
    hashes = _gates(spec, state, attempts, authority, evidence)
    return {
        "kind": KIND,
        "command": payload,
        "authority": authority,
        "controller": controller,
        "spec": spec,
        "execution_spec": spec,
        "original_row": row,
        "original_recovery": previous,
        "closed": closed,
        "state": state,
        "attempts": attempts,
        "effects": effects,
        "resources": resources,
        "retained_hashes": hashes,
        "candidate": state["candidate"],
        "publication": state["pull_request"],
        "maximum_iteration": 4,
    }


def custody(db, recovery):
    root = Path(recovery["spec"]["state_dir"]) / "investigation-adjudication"
    _ancestors(root / "intent.json")
    seal = read_private(root / "intent.json")
    command = db.execute(
        "SELECT request_digest FROM delivery_commands WHERE command_id=?",
        (recovery["command"]["command_id"],),
    ).fetchone()
    if (
        canonical_json(seal) != canonical_json(recovery)
        or not command
        or command[0] != digest({"run_id": recovery["spec"]["run_id"], **recovery["command"]})
    ):
        raise ValueError("adjudication immutable durable admission changed")
    _authority(recovery["command"], recovery["spec"]["run_id"])
    return seal


def readback(store, spec, recovery):
    with store._connect() as db:
        custody(db, recovery)
        work_binding(store, spec, db)
        claim = store.state.claim_for(db, spec["work_id"])
    if claim is not None and claim["owner"] != f"external:devflow:{spec['run_id']}":
        raise ValueError("adjudication claim became foreign")
    _controller(store, spec, recovery["command"])
    authority, evidence = _authority(recovery["command"], spec["run_id"])
    _historical(store, spec, recovery["original_recovery"], authority)
    _gates(spec, recovery["state"], recovery["attempts"], authority, evidence)
    for name in ("manifest.json", "finalization.json"):
        path = Path(spec["state_dir"]) / "investigation-adjudication/predecessor-resources" / name
        _bytes(
            path,
            recovery["resources"][
                "manifest_sha256" if name == "manifest.json" else "finalization_sha256"
            ],
        )
    from .delivery_repair import published_identity

    published_identity(DeliveryBroker(store, spec), recovery["candidate"], recovery["publication"])
    return {
        "state": "adjudicated",
        "raw_status": "findings",
        "additional_native_execution": False,
        "disposition": authority["accepted_disposition"],
        "controller": recovery["controller"],
    }


def admit(store, run_id, payload, *, preflight=False):
    _request(payload)
    from .delivery_native_guard import reject_nested_controller

    reject_nested_controller()
    command_digest = digest({"run_id": run_id, **payload})
    with store._connect() as db:
        prior = db.execute(
            "SELECT * FROM delivery_commands WHERE command_id=?", (payload["command_id"],)
        ).fetchone()
        row = db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (run_id,)
        ).fetchone()
    if prior:
        if prior["request_digest"] != command_digest:
            raise ValueError("adjudication command ID belongs to different inputs")
        recovery = json.loads(row[0])
        readback(store, recovery["spec"], recovery)
        return {**json.loads(prior["response_json"]), "existing": True, "preflight": preflight}
    seal = _snapshot(store, run_id, payload)
    root = Path(seal["spec"]["state_dir"]) / "investigation-adjudication"
    intent = root / "intent.json"
    _ancestors(intent, allow_missing=True)
    if intent.exists() and canonical_json(read_private(intent)) != canonical_json(seal):
        raise ValueError("adjudication orphan intent does not bind the same stopped request")
    response = {
        "run_id": run_id,
        "workflow_id": f"delivery-{run_id}-adjudication-1",
        "phase": "investigation_adjudication_queued",
        "authorized_through_iteration": 4,
        "additional_iterations": 0,
        "existing": False,
        "dashboard_url": f"{store.config.dashboard_url}/runs/{run_id}",
    }
    if preflight:
        return {
            **response,
            "preflight": True,
            "precheck_sha256": digest(seal),
            "raw_status": "findings",
            "remaining_activities": sorted(ACTIVITIES),
        }
    from .delivery_preparation import _lock

    with _lock(root / "controller.lock"):
        fresh = _snapshot(store, run_id, payload)
        if canonical_json(fresh) != canonical_json(seal):
            raise ValueError("whole adjudication request changed before sealing")
        private_directory(root)
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if (
                dict(current) != seal["original_row"]
                or store.state.claim_for(db, seal["spec"]["work_id"]) is not None
            ):
                raise ValueError("adjudication stopped checkpoint or claim changed")
            work_binding(store, seal["spec"], db)
            # Transaction rollback releases this owning reacquisition if any immutable write fails.
            store.state.claim_work(
                db,
                seal["spec"]["work_id"],
                f"external:devflow:{run_id}",
                store.config.dashboard_url,
            )
            _immutable(intent, seal)
            private_directory(root / "predecessor-resources")
            for name, key in (
                ("manifest.json", "manifest_sha256"),
                ("finalization.json", "finalization_sha256"),
            ):
                source = Path(seal["spec"]["state_dir"]) / "resources" / name
                raw = _bytes(source, seal["resources"][key])
                _immutable(root / "predecessor-resources" / name, json.loads(raw), raw=raw)
            db.execute(
                "UPDATE delivery_runs SET phase='investigation_adjudication_queued',"
                "execution_state='queued',"
                "outcome=NULL,error=NULL,revision=revision+1,workflow_id=?,recovery_json=?,"
                "updated_at=? WHERE run_id=?",
                (response["workflow_id"], canonical_json(seal), store.state.now(), run_id),
            )
            db.execute(
                "UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=? "
                "WHERE run_id=?",
                (store.state.now(), run_id),
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (payload["command_id"], run_id, command_digest, canonical_json(response)),
            )
            store._event(
                db,
                run_id,
                current["revision"] + 1,
                response["phase"],
                "Independent investigation disposition retained; controller final gates queued",
                {
                    "raw_status": "findings",
                    "authority_sha256": payload["authority_sha256"],
                    "iteration": 4,
                },
            )
    return response
