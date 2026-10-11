"""Retained revision workers keep edit authority while adopting parent-admitted gates."""

from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest
from test_delivery_feature_gates import BULK, PAGINATION
from test_delivery_feature_gates import gate_project as gate_project

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry, revised_worker_spec, worker_spec
from devflow_temporal.delivery_feature_revision_recovery import (
    _derive,
    validate_custody,
    validate_revision_recovery,
)
from devflow_temporal.delivery_feature_revisions import PREFIX
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_source_scope import authority, require_authorized


def retained_worker(parent, plan, chunk_id="api1", *, kind="chunk"):
    """Create the original legacy assignment before the feature adopts v2."""
    legacy_parent = deepcopy(parent)
    legacy_plan = deepcopy(plan)
    legacy_plan.pop("version")
    legacy_plan.pop("final_gates")
    for stream in legacy_plan["workstreams"]:
        for chunk in stream["chunks"]:
            chunk["allowed_paths"] = chunk.pop("expected_paths")
            chunk.pop("gates")
    legacy_parent["accepted_plan"] = canonical_json(legacy_plan)
    chunk = next(c for c in ordered_chunks(legacy_plan) if c["id"] == chunk_id)
    worker = worker_spec(
        legacy_parent,
        chunk,
        {"url": "https://github.com/example/fixture/issues/" + str(chunk["issue_number"])},
        kind=kind,
        base_sha=parent["base_sha"],
        base_branch="original-stack-base",
    )
    worker["continuation"] = {
        "session_id": "original-product-session",
        "candidate_id": "original-product-candidate",
        "pull_request": {"number": 20, "head": "original-tip"},
    }
    return worker


def revision_identity(plan):
    return {"plan_revision": 2, "plan_digest": digest(plan)}


@pytest.mark.parametrize("parent_scope", [False, True], ids=["legacy-parent", "scoped-parent"])
@pytest.mark.parametrize("worker_scope", [False, True], ids=["legacy-worker", "scoped-worker"])
def test_revision_keeps_retained_authority_and_native_policy_while_admitting_full_plan(
    gate_project, parent_scope, worker_scope,
):
    parent, plan, _ = gate_project
    parent["policy"].update(
        checks=[{"id": "lint", "argv": ["git", "diff", "--check"]}],
        prepublish_checks=[{"id": "prepublish", "argv": ["git", "diff", "--check"]}],
        capacity={"cpus": 8},
        max_repairs=10,
        native_identity={"runtime_payload_sha256": "parent-runtime"},
        roles={"implement": {"model": "parent-model", "effort": "high"}},
    )
    plan["workstreams"][0]["chunks"][0]["gates"] += [
        {"stage": "checks", "recipe_id": "lint", "selectors": []},
        {"stage": "prepublish_checks", "recipe_id": "prepublish", "selectors": []},
    ]
    worker = retained_worker(parent, plan)
    worker["policy"].update(
        checks=[],
        prepublish_checks=[],
        capacity={"cpus": 2},
        max_repairs=4,
        native_identity={"runtime_payload_sha256": "worker-runtime"},
        roles={"implement": {"model": "worker-model", "effort": "medium"}},
        host_sandbox="retained-profile",
        config_overrides={"network": "retained-restriction"},
        runtime_dependencies=["retained-runtime"],
        provider_max_attempts=1,
        codex_bin="/original/provider",
        required_ci=["worker-ci"],
        worker_only_policy={"constraint": "retained"},
        security_binding_sha256="old-security-proof",
        environment_proof_sha256="old-environment-proof",
    )
    if parent_scope:
        parent["policy"]["allowed_paths"] = []
        parent["policy"]["source_scope"] = {
            "version": 1,
            "allowed_roots": ["."],
            "allowed_files": [],
            "protected_paths": ["secrets", ".git", ".codex"],
        }
    if worker_scope:
        worker["policy"]["allowed_paths"] = []
        worker["policy"]["source_scope"] = {
            "version": 1,
            "allowed_roots": ["src"],
            "allowed_files": ["README.md"],
            "protected_paths": ["src/private", ".git", ".codex"],
        }
    before = canonical_json({"parent": parent, "worker": worker, "plan": plan})
    revised = revised_worker_spec(
        parent, plan, "api1", expected_revision=revision_identity(plan), worker=worker,
    )
    require_authorized(revised["policy"], ["README.md"])
    with pytest.raises(ValueError, match="job-list-pagination.spec.ts"):
        require_authorized(revised["policy"], [PAGINATION])
    with pytest.raises(ValueError, match="src/private/key.py"):
        require_authorized(revised["policy"], ["src/private/key.py"])
    if worker_scope:
        require_authorized(revised["policy"], ["src/new.py"])
    assert authority(revised["policy"]) == authority(worker["policy"])
    assert revised["expected_paths"] == ["README.md"]
    # The future web chunk is valid for the parent, even though the original
    # API worker cannot edit its file. Expected paths never authorize that file.
    assert plan["workstreams"][1]["chunks"][0]["expected_paths"] == [PAGINATION]
    for key in (
        "capacity", "max_repairs", "native_identity", "roles", "host_sandbox",
        "config_overrides", "runtime_dependencies", "provider_max_attempts", "codex_bin",
        "required_ci", "worker_only_policy", "allowed_paths", "source_scope",
    ):
        assert revised["policy"].get(key) == worker["policy"].get(key)
    assert revised["policy"]["checks"] == parent["policy"]["checks"]
    assert revised["policy"]["prepublish_checks"] == parent["policy"]["prepublish_checks"]
    assert revised["policy"]["browser_qa"]["required_selectors"] == [BULK]
    assert "e2e/tests/job-list-pagination.spec.ts" not in revised["policy"]["browser_qa"]["argv"]
    assert "security_binding_sha256" not in revised["policy"]
    assert "environment_proof_sha256" not in revised["policy"]
    for key in ("run_id", "work_id", "command_id", "request_digest", "checkout", "state_dir",
                "branch", "base_sha", "publication_base_ref", "feature_worker", "continuation"):
        assert revised[key] == worker[key]
    assert json.loads(revised["accepted_plan"])["integration"] == json.loads(
        worker["accepted_plan"]
    )["integration"]
    assert revised["policy_digest"] == digest(revised["policy"])
    assert canonical_json({"parent": parent, "worker": worker, "plan": plan}) == before


@pytest.mark.parametrize("kind", [None, "build", "chunk"], ids=["no-worker", "build", "final"])
def test_new_policy_and_final_gate_selection_keep_existing_semantics(gate_project, kind):
    parent, plan, _ = gate_project
    plan["workstreams"][1]["chunks"][0]["gates"][0]["selectors"] = [PAGINATION]
    worker = retained_worker(parent, plan, "web1", kind=kind) if kind else None
    before = canonical_json({"parent": parent, "worker": worker, "plan": plan})
    revised = revised_worker_spec(
        parent, plan, "web1", expected_revision=revision_identity(plan), worker=worker,
    )
    assert set(revised["policy"]["browser_qa"]["required_selectors"]) == (
        {BULK, PAGINATION} if kind == "chunk" else {PAGINATION}
    )
    accepted = json.loads(revised["accepted_plan"])
    assert accepted["acceptance"] == plan["workstreams"][1]["chunks"][0]["acceptance"]
    if kind == "chunk":
        assert accepted["feature_acceptance"] == plan["acceptance"]
    else:
        assert "feature_acceptance" not in accepted
    if worker is None:
        assert authority(revised["policy"]) == authority(parent["policy"])
        require_authorized(revised["policy"], [PAGINATION])
    else:
        assert authority(revised["policy"]) == authority(worker["policy"])
    assert canonical_json({"parent": parent, "worker": worker, "plan": plan}) == before


@pytest.mark.parametrize("affected", [False, True], ids=["unaffected", "affected"])
def test_real_revision_recovery_custody_preserves_original_worker_authority(
    gate_project, tmp_path, affected,
):
    parent, plan, _ = gate_project
    shared = registry(parent)
    owner = shared.claim(
        {"issue": {"id": "I_parent", "repository_id": "R_fixture", "url": parent["issue_url"]}},
        parent["run_id"], str(tmp_path / "local-delivery.sqlite3"),
    )
    parent["feature_delivery"]["owner"] = owner
    worker = retained_worker(parent, plan)
    worker["policy"].update(capacity={"cpus": 2}, native_identity={"sha256": "original-runtime"})
    identity = revision_identity(plan)
    receipt = {
        "revision_id": "fixture-adopted-revision",
        "owner": owner,
        "identity": identity,
        "plan": plan,
        "affected_chunks": ["api1"] if affected else [],
        "implementation_chunks": [],
    }
    shared.checkpoint(owner, PREFIX + "adopted:2", receipt)
    parent["feature_plan_revision"] = identity
    saved = canonical_json({"parent": parent, "worker": worker, "plan": plan})
    derived = _derive(parent, receipt, worker)
    admission = {
        "revision_id": receipt["revision_id"],
        "plan_identity": identity,
        "parent_run_id": parent["run_id"],
        "predecessor_spec_digest": digest(worker),
        "derived_spec_digest": digest(derived),
    }
    store = SimpleNamespace(
        effective_spec=lambda run_id: deepcopy(parent) if run_id == "parent" else None,
    )
    assert validate_revision_recovery(store, worker, admission) == derived
    recovery = {
        "predecessor_spec": worker, "execution_spec": derived, "feature_plan_revision": admission,
    }
    validate_custody(store, recovery)
    with pytest.raises(ValueError, match="job-list-pagination.spec.ts"):
        require_authorized(derived["policy"], [PAGINATION])
    assert derived["continuation"] == worker["continuation"]
    assert derived["policy"]["native_identity"] == worker["policy"]["native_identity"]
    assert derived["policy"]["capacity"] == worker["policy"]["capacity"]
    assert derived["feature_worker"]["parent_run_id"] == parent["run_id"]
    assert derived["feature_delivery"]["owner"] == owner
    if affected:
        assert derived["policy"]["browser_qa"]["required_selectors"] == [BULK]
    else:
        assert derived["policy"] == worker["policy"]
        assert derived["accepted_plan"] == worker["accepted_plan"]
    for changed_field in ("allowed_paths", "capacity"):
        tampered = deepcopy(recovery)
        tampered["execution_spec"]["policy"][changed_field] = deepcopy(
            parent["policy"].get(changed_field, {"cpus": 8})
        )
        with pytest.raises(OwnershipConflict, match="derived authority"):
            validate_custody(store, tampered)
    tampered = deepcopy(admission)
    tampered["derived_spec_digest"] = "forged-derivation"
    with pytest.raises(OwnershipConflict, match="immutable derivation"):
        validate_revision_recovery(store, worker, tampered)
    assert canonical_json({"parent": parent, "worker": worker, "plan": plan}) == saved
