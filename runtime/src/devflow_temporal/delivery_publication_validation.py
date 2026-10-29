"""Explicit current-head validation of an existing ordinary documentation delivery."""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_filesystem_custody import matches
from .delivery_metadata_recovery import _immutable, preserve_resources
from .delivery_policy_recovery import _rows, work_binding
from .delivery_resources import (
    RunResources,
    private_directory,
    read_private,
    write_private,
)
from .delivery_source_scope import outside_scope

KIND = "published_head_validation"
HOLD = {"do not merge yet", "status: deferred", "status: cancelled", "wontfix", "won't fix"}


def namespace(recovery):
    return "publication-validation-" + recovery["command"]["expected_pr_head"]


def custody(db, recovery):
    spec = recovery["execution_spec"]
    root = Path(spec["state_dir"]) / namespace(recovery)
    saved = db.execute("SELECT request_digest FROM delivery_runs WHERE run_id=?",
                       (spec["run_id"],)).fetchone()
    if (not saved or saved[0] != spec["request_digest"]
            or recovery["command"]["additional_iterations"] != 0
            or recovery["original_spec"]["request_digest"] != saved[0]
            or canonical_json(read_private(root / "admission.json"))
            != canonical_json(recovery)):
        raise ValueError("publication validation lost its immutable admission")


def effective_spec(store, recovery):
    with store._connect() as db:
        custody(db, recovery)
    return recovery["execution_spec"]


def _remote(spec, old, expected_head, *, check_head=True):
    from .delivery_project_sync import GitHub

    gh = GitHub()
    pr = gh.command("pr", "view", str(old["number"]), "--repo", spec["github_repo"],
                    "--json", "number,url,state,headRefOid,headRefName,baseRefName,isDraft,labels")
    issue = gh.issue(spec["issue_url"])
    labels = {label.casefold() for label in issue.get("labels", [])}
    labels.update(label["name"].casefold() for label in pr["labels"])
    base = spec.get("publication_base_ref")
    if (pr["state"] != "OPEN" or pr["isDraft"] or issue["state"] != "OPEN"
            or pr["url"].casefold() != old["url"].casefold()
            or (check_head and pr["headRefOid"] != expected_head)
            or pr["headRefName"] != spec["branch"]
            or (base is not None and pr["baseRefName"] != base)
            or labels & HOLD):
        raise ValueError("current publication changed or has an owner hold")
    return pr


def _checkpoint(store, run_id, payload, *, observation=False):
    from .delivery_execution_config import observation_mode

    if observation:
        with observation_mode():
            return _checkpoint_inner(store, run_id, payload, observation=True)
    return _checkpoint_inner(store, run_id, payload, observation=False)


def _checkpoint_inner(store, run_id, payload, *, observation):
    spec = store.effective_spec(run_id)
    if (spec.get("feature_delivery") or spec.get("authorized_endpoint") != "published_unmerged"
            or spec.get("provider") not in {"codex", "fake"}
            or not store.owns_execution(spec)):
        raise ValueError("validation requires this service's ordinary unmerged publication")
    row, attempts, effects, claim = _rows(store, run_id)
    with store._connect() as db:
        from .delivery_settings import require_repository_access

        require_repository_access(store, db, spec["github_repo"])
        binding = work_binding(store, spec, db)
    previous = json.loads(row["recovery_json"] or "null")
    closed = store._completed_temporal_result(
        run_id, workflow_id=row["workflow_id"] or "delivery-" + run_id,
        preserve=not observation)
    state = closed["result"]
    candidate, pr = (json.loads(row[k] or "null") for k in ("candidate_json", "pr_json"))
    if (row["outcome"] not in {"delivered", "blocked"} or row["cleanup"] != "confirmed"
            or row["execution_state"] not in {"terminal", "blocked"}
            or closed["request_digest"] != row["request_digest"]
            or closed["recovery_digest"] != (digest(previous) if previous else None)
            or state.get("run_id") != run_id or state.get("outcome") != row["outcome"]
            or state.get("revision") != row["protocol_revision"]
            or not candidate or state.get("candidate") != candidate
            or not pr or state.get("pull_request") != pr
            or any(a["state"] != "finished" or a["cleanup"] != "confirmed" for a in attempts)
            or any(e["state"] != "complete" for e in effects)
            or (claim and claim["owner"] != "external:devflow:" + run_id)):
        raise ValueError("publication validation checkpoint is stale or not settled")
    checks = state.get("checks", {})
    if any(checks.get(stage, {}).get("state") != "passed"
           for stage in ("prepublish", "local", "review", "qa")):
        raise ValueError("validation requires the predecessor's completed acceptance gates")
    implementation = next((r for r in reversed(state.get("roles", []))
                           if r.get("role") == "implement" and r.get("status") == "pass"), None)
    if not implementation or not implementation.get("session_id"):
        raise ValueError("validation has no retained implementation session")
    if spec["provider"] == "codex":
        from .delivery_repair import confirmed_native_cleanup

        if observation:
            from .delivery_resources import observe_finalized_resources

            observe_finalized_resources(spec)
        else:
            confirmed_native_cleanup(spec)
    remote = _remote(spec, pr, payload["expected_pr_head"], check_head=False)
    from .delivery_project_sync import GitHub

    comparison = GitHub().command("api", f"repos/{spec['github_repo']}/compare/"
                                  f"{candidate['head']}...{remote['headRefOid']}")
    files = comparison.get("files", [])
    paths = [f["filename"] for f in files]
    if (comparison.get("status") != "ahead"
            or comparison["merge_base_commit"]["sha"] != candidate["head"]
            or not 1 <= len(paths) <= 64
            or any(not p.startswith("docs/") or not p.endswith((".md", ".mdx")) for p in paths)
            or any(f["status"] not in {"modified", "added", "removed"} for f in files)
            or outside_scope(spec["policy"], paths)):
        raise ValueError("validation requires an in-scope documentation-only fast-forward")
    return {"row": row, "attempts": attempts, "effects": effects, "claim": claim,
            "work_binding": binding, "closed": closed, "original_spec": spec,
            "original_recovery": previous, "state": state, "candidate": candidate,
            "publication": pr, "paths": paths, "base_branch": remote["baseRefName"],
            "session_id": implementation["session_id"], "remote_head": remote["headRefOid"]}


def _assessment(seal, payload):
    from .delivery_lifecycle import require_transition

    return require_transition({
        "operation": KIND, "owner": "publication_validation", "owner_proof": True,
        "proof_digest": digest(seal), "normalized_lifecycle": "stopped",
        "revision": seal["row"]["protocol_revision"],
        "projection_revision": seal["row"]["revision"],
        "candidate_id": seal["candidate"]["id"], "candidate_head": seal["remote_head"],
        "publication_number": seal["publication"]["number"], "publication_required": True,
        "comparisons": [("revision", "expected_revision", "revision_changed"),
                        ("projection_revision", "expected_projection_revision",
                         "projection_revision_changed"),
                        ("candidate_id", "expected_candidate_id", "candidate_changed"),
                        ("candidate_head", "expected_pr_head", "candidate_head_changed"),
                        ("publication_number", "expected_pr_number", "publication_changed")],
    }, payload)


def assess(store, run_id, payload):
    from .delivery_lifecycle import RecoveryRefused, evaluate_transition

    try:
        result = admit(store, run_id, payload, preflight=True)
        if isinstance(result, dict) and isinstance(result.get("lifecycle_assessment"), dict):
            return result["lifecycle_assessment"]
    except RecoveryRefused as exc:
        return exc.assessment
    except (ValueError, RuntimeError, OSError, KeyError, TypeError):
        pass
    return evaluate_transition({"operation": KIND, "owner": "publication_validation",
                                "comparisons": [], "predicates": [{
        "code": "publication_validation_proof_unavailable", "satisfied": None,
        "missing_or_unknown_fact": "settled publication owner, cleanup and current head evidence",
        "next_action": "inspect the publication validation owner evidence",
    }]}, {})


def restore_checkout(store, seal, root):
    """Recreate only an absent, settled run checkout at its retained local commit."""
    spec = seal["original_spec"]
    broker = DeliveryBroker(store, spec)
    if os.path.lexists(broker.checkout):
        return
    private_directory(broker.checkout.parent)
    if (broker.source.resolve() != broker.source
            or _git(broker.source, "remote", "get-url", "origin") != spec["origin_url"]):
        raise ValueError("validation source repository changed")
    branch = spec.get("local_branch", spec["branch"])
    if _git(broker.source, "rev-parse", "refs/heads/" + branch) != seal["candidate"]["head"]:
        raise ValueError("retained local branch differs from the completed candidate")
    preserve_resources(root, spec)
    resources = RunResources(spec) if spec.get("resource_cleanup_version") == 1 else None
    if resources:
        with resources.locked() as manifest:
            old = manifest["roots"].get(str(broker.checkout), {})
            if (old.get("kind") != "checkout" or os.path.lexists(broker.checkout)
                    or manifest.get("finalization", {}).get("resource_cleanup") != "confirmed"):
                raise ValueError("checkout restoration lacks settled resource ownership")
            _immutable(root / "checkout-restoration.json", {
                "path": str(broker.checkout), "head": seal["candidate"]["head"],
                "predecessor": deepcopy(old), "checkpoint_digest": digest(seal)})
            old.update(identity=None, state="allocated", generation=old.get("generation", 0) + 1)
            old.pop("receipt", None)
            write_private(resources.manifest, manifest)
    _git(broker.source, "worktree", "add", str(broker.checkout), branch)
    if resources:
        resources.created(broker.checkout)
    if broker.candidate() != seal["candidate"]:
        raise ValueError("restored checkout differs from the completed candidate")


def preserve_validation_resources(root, seal):
    spec = seal["original_spec"]
    restoration = root / "checkout-restoration.json"
    if not restoration.exists():
        preserve_resources(root, spec)
        return
    saved = read_private(restoration)
    old = read_private(root / "predecessor-resources/manifest.json")
    resources = RunResources(spec, read_only=True)
    current = read_private(resources.manifest)
    entry = current["roots"][spec["checkout"]]
    if (saved["checkpoint_digest"] != digest(seal) or saved["path"] != spec["checkout"]
            or saved["head"] != seal["candidate"]["head"]
            or old["roots"][spec["checkout"]] != saved["predecessor"]
            or not matches(spec, Path(spec["checkout"]), entry.get("identity"))
            or entry.get("generation") != saved["predecessor"].get("generation", 0) + 1
            or resources.root.joinpath("finalization.json").read_bytes()
            != root.joinpath("predecessor-resources/finalization.json").read_bytes()):
        raise ValueError("checkout restoration changed its predecessor evidence")
    current["roots"][spec["checkout"]] = saved["predecessor"]
    if current != old:
        raise ValueError("resources changed outside the restored checkout")


def admit(store, run_id, payload, *, preflight=False):
    fields = {"continuation_kind", "command_id", "expected_revision",
              "expected_projection_revision", "expected_candidate_id", "expected_pr_number",
              "expected_pr_head", "additional_iterations"}
    if (not isinstance(payload, dict) or set(payload) != fields
            or payload.get("continuation_kind") != KIND
            or not isinstance(payload.get("command_id"), str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", payload["command_id"])
            or type(payload.get("additional_iterations")) is not int
            or payload["additional_iterations"] != 0
            or any(type(payload.get(k)) is not int or payload[k] < 1 for k in (
                "expected_revision", "expected_projection_revision", "expected_pr_number"))
            or not isinstance(payload.get("expected_candidate_id"), str)
            or not re.fullmatch(r"[0-9a-f]{64}", payload["expected_candidate_id"])
            or not isinstance(payload.get("expected_pr_head"), str)
            or not re.fullmatch(r"[0-9a-f]{40}", payload["expected_pr_head"])):
        raise ValueError("validation requires exact publication identities and no repair grant")
    command_digest = digest({"run_id": run_id, **payload})
    with store._connect() as db:
        prior = db.execute("SELECT * FROM delivery_commands WHERE command_id=?",
                           (payload["command_id"],)).fetchone()
    if prior:
        if prior["run_id"] != run_id or prior["request_digest"] != command_digest:
            raise ValueError("command ID already belongs to different validation inputs")
        return json.loads(prior["response_json"])
    seal = _checkpoint(store, run_id, payload, observation=True)
    assessment = _assessment(seal, payload)
    if preflight:
        return {"run_id": run_id, "preflight": True, "paths": seal["paths"],
                "pr_number": payload["expected_pr_number"], "head": payload["expected_pr_head"],
                "additional_iterations": 0, "lifecycle_assessment": assessment}
    original = seal["original_spec"]
    root = Path(original["state_dir"]) / ("publication-validation-" + payload["expected_pr_head"])
    from .delivery_preparation import _lock

    with _lock(root / "admission"):
        fresh = _checkpoint(store, run_id, payload, observation=True)
        _assessment(fresh, payload)
        if fresh != seal:
            raise ValueError("publication checkpoint changed before effects")
        if original["provider"] == "codex":
            from .delivery_repair import confirmed_native_cleanup

            confirmed_native_cleanup(original)
        private_directory(root)
        from .delivery_gate_retry import prepare_runtime
        from .delivery_session_custody import implementation_generation

        execution = prepare_runtime(original, root, command_digest, digest(seal))
        execution["implementation_role_home_generation"] = implementation_generation(
            original, seal["original_recovery"])
        execution["role_home_generation"] = "publication-" + payload["expected_pr_head"]
        # Admission owns fresh evidence namespaces; the frozen product policy is unchanged.
        execution["publication_validation_head"] = payload["expected_pr_head"]
        restore_checkout(store, seal, root)
        broker = DeliveryBroker(store, original)
        local_head = _git(broker.checkout, "rev-parse", "HEAD")
        retained = root / "predecessor.json"
        if (local_head not in {seal["candidate"]["head"], payload["expected_pr_head"]}
                or _git(broker.checkout, "status", "--porcelain", "--untracked-files=no")):
            raise ValueError("retained implementation checkout changed before validation")
        _git(broker.checkout, "fetch", "--no-tags", "origin", payload["expected_pr_head"])
        paths = _git(broker.checkout, "diff", "--name-only", seal["candidate"]["head"],
                     payload["expected_pr_head"]).splitlines()
        if set(paths) != set(seal["paths"]):
            raise ValueError("fetched documentation delta differs from admission")
        for path in paths:
            for head in (seal["candidate"]["head"], payload["expected_pr_head"]):
                tree = _git(broker.checkout, "ls-tree", head, "--", path)
                if tree and not tree.startswith("100644 blob "):
                    raise ValueError("documentation validation cannot adopt executable or linked "
                                     "files")
        if digest(_checkpoint(store, run_id, payload, observation=True)) != digest(seal):
            raise ValueError("publication checkpoint changed during native preparation")
        preserve_validation_resources(root, seal)
        _immutable(retained, seal)
        _git(broker.checkout, "merge", "--ff-only", "--no-edit", payload["expected_pr_head"])
        candidate = DeliveryBroker(store, execution).candidate()
        recovery = {"kind": KIND, "command": payload, "seal": seal,
                    "original_spec": original, "original_recovery": seal["original_recovery"],
                    "execution_spec": execution, "state": seal["state"],
                    "candidate": candidate, "session_id": seal["session_id"],
                    "publication": {**seal["publication"], "head": candidate["head"],
                                    "candidate": candidate}}
        _immutable(root / "admission.json", recovery)
        workflow_id = f"delivery-{run_id}-validate-{payload['expected_pr_head']}"
        response = {"run_id": run_id, "phase": "gates_retry_queued", "workflow_id": workflow_id,
                    "candidate_id": candidate["id"], "head": candidate["head"],
                    "pr_number": payload["expected_pr_number"], "additional_iterations": 0}
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if (dict(row) != seal["row"]
                    or work_binding(store, original, db) != seal["work_binding"]):
                raise ValueError("publication validation lost its stopped owner")
            for table, key, expected in (("delivery_attempts", "job_key", seal["attempts"]),
                                         ("delivery_effects", "effect_key", seal["effects"])):
                actual = [dict(item) for item in db.execute(
                    f"SELECT * FROM {table} WHERE run_id=? ORDER BY {key}", (run_id,))]
                if actual != sorted(expected, key=lambda item: item[key]):
                    raise ValueError("publication validation predecessor evidence changed")
            store.state.claim_work(db, original["work_id"], "external:devflow:" + run_id,
                                   store.config.dashboard_url)
            db.execute("UPDATE delivery_runs SET phase='gates_retry_queued',"
                       "execution_state='queued',"
                       "outcome=NULL,error=NULL,revision=revision+1,workflow_id=?,recovery_json=?,"
                       "updated_at=? WHERE run_id=?",
                       (workflow_id, canonical_json(recovery), store.state.now(), run_id))
            db.execute("UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=? "
                       "WHERE run_id=?", (store.state.now(), run_id))
            db.execute("INSERT INTO delivery_commands VALUES (?,?,?,?)",
                       (payload["command_id"], run_id, command_digest, canonical_json(response)))
            store._event(db, run_id, row["revision"] + 1, "publication_validation_queued",
                         "Validating the updated documentation in the existing PR", response)
        return response


def readback(store, spec, recovery):
    with store._connect() as db:
        custody(db, recovery)
    broker = DeliveryBroker(store, spec)
    if broker.candidate() != recovery["candidate"]:
        raise ValueError("admitted validation source changed")
    remote = _remote(spec, recovery["seal"]["publication"], recovery["candidate"]["head"])
    if remote["baseRefName"] != recovery["seal"]["base_branch"]:
        raise ValueError("publication target branch changed")
    return deepcopy(recovery["publication"])
