"""Real plan-comparison production through native role preparation, without launch."""
from __future__ import annotations

import hashlib
import json
import os
import tomllib
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_github_contract import plan

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_feature_revision_roles import revision_diff
from devflow_temporal.delivery_plan_model import migrate_plan_v1
from devflow_temporal.delivery_resources import private_directory, write_private
from devflow_temporal.delivery_sandbox import _native_role_home, prepare_native_role


@pytest.fixture
def native_review(tmp_path):
    state = tmp_path / "run"
    workspace = state / "gates/0/review"
    private_directory(workspace)
    (workspace / ".git").mkdir()
    auth = tmp_path / "synthetic-auth.json"
    write_private(auth, {"fixture": "no provider credentials"})
    previous = plan()
    proposed = migrate_plan_v1(
        previous, {s["id"]: {"number": s["issue_number"]} for s in previous["workstreams"]},
        {c["id"]: [] for s in previous["workstreams"] for c in s["chunks"]}, [],
    )
    proposed["workstreams"][0]["chunks"][0]["verification"].append("Check current prerequisites")
    candidate = {"id": "c" * 64, "base_sha": "a" * 40, "head": "b" * 40}
    context = {
        "revision_id": "revision-" + "1" * 24,
        "namespace": "plan-revisions/revision-" + "1" * 24,
        "candidate_id": candidate["id"], "old_plan": previous, "proposed_plan": proposed,
        "old_identity": {"plan_revision": 1, "plan_digest": digest(previous)},
        "proposed_plan_sha256": digest(proposed),
    }
    spec = {
        "run_id": "run", "state_dir": str(state), "checkout": str(workspace),
        "provider": "codex", "base_sha": candidate["base_sha"],
        "policy": {"host_sandbox": "native-profile", "execution_backend": "native-macos",
                   "codex_bin": "/usr/bin/false", "codex_auth_path": str(auth)},
    }
    request = {"spec": spec, "role": "review", "iteration": 0,
               "candidate": candidate, "workspace": str(workspace), "resume_session": None,
               "revision_context": context}
    broker = SimpleNamespace(evidence_dir=state)
    return broker, request


def prepare(request, attempt_id="review-attempt"):
    attempt = Path(request["spec"]["state_dir"]) / "attempts" / attempt_id
    # Pin the admitted historical recipe so fixture scratch stays under its run.
    write_private(attempt / "native-environment.json", {"version": 1})
    return prepare_native_role(request, attempt)


def profile(env):
    path = Path(env["CODEX_HOME"]) / "config.toml"
    return path, tomllib.loads(path.read_text())


def test_revision_producer_enters_real_native_preparation_read_only_and_repeats(native_review):
    broker, request = native_review
    request["review_diff"] = revision_diff(broker, request)
    artifact = Path(request["review_diff"]["path"])
    before = artifact.read_bytes()
    name, env = prepare(request)
    config_path, config = profile(env)
    files = config["permissions"][name]["filesystem"]
    assert files[request["workspace"]] == "read"
    assert files[str(Path(request["workspace"]) / ".git")] == "deny"
    assert files[str(artifact)] == "read"
    assert str(broker.evidence_dir) not in files and str(artifact.parent) not in files
    assert files[env["CODEX_HOME"]] == "deny"
    assert config["features"]["multi_agent"] is False
    assert config["agents"]["enabled"] is False
    assert config_path.is_relative_to(_native_role_home(request))
    assert not config_path.is_relative_to(Path(request["workspace"]))
    frozen_config = config_path.read_bytes()
    frozen_auth = (config_path.parent / "auth.json").read_bytes()
    assert revision_diff(broker, request) == request["review_diff"]
    assert prepare(request) == (name, env)
    assert prepare(deepcopy(request), "replayed-attempt") == (name, env)
    assert artifact.read_bytes() == before
    assert config_path.read_bytes() == frozen_config
    assert (config_path.parent / "auth.json").read_bytes() == frozen_auth
    assert hashlib.sha256(before).hexdigest() == request["review_diff"]["sha256"]
    assert artifact.stat().st_mode & 0o777 == 0o600 and artifact.stat().st_nlink == 1


def test_retained_comparison_receipt_has_same_bytes_and_native_replay(native_review):
    broker, request = native_review
    request["review_diff"] = revision_diff(broker, request)
    # Retained generation-8 envelopes predate any added outer candidate binding.
    request["review_diff"].pop("candidate_id", None)
    artifact = Path(request["review_diff"]["path"])
    before = artifact.read_bytes()
    receipt = json.loads(before)
    assert set(receipt) == {
        "version", "revision_id", "candidate_id", "old_plan_sha256", "proposed_plan_sha256", "diff",
    }
    first = prepare(request)
    assert prepare(deepcopy(request)) == first
    assert artifact.read_bytes() == before


def test_later_revision_review_uses_distinct_home_without_rewriting_retained_home(native_review):
    broker, request = native_review
    request["review_diff"] = revision_diff(broker, request)
    first_home = _native_role_home(request)
    _, env = prepare(request)
    original_config, _ = profile(env)
    original_bytes = original_config.read_bytes()
    original_artifact = Path(request["review_diff"]["path"]).read_bytes()
    later = deepcopy(request)
    context = later["revision_context"]
    context["revision_id"] = "revision-" + "2" * 24
    context["namespace"] = "plan-revisions/" + context["revision_id"]
    later["review_diff"] = revision_diff(broker, later)
    second_home = _native_role_home(later)
    assert second_home != first_home
    _, later_env = prepare(later, "later-attempt")
    later_config, later_value = profile(later_env)
    later_files = later_value["permissions"]["devflow-role"]["filesystem"]
    assert later_files[later["review_diff"]["path"]] == "read"
    assert later_config != original_config
    assert prepare(request) == ("devflow-role", env)
    assert original_config.read_bytes() == original_bytes
    assert Path(request["review_diff"]["path"]).read_bytes() == original_artifact
    # Original intake and product-gate role homes keep their established paths.
    ordinary = {k: v for k, v in request.items() if k != "revision_context"}
    assert _native_role_home(ordinary) == broker.evidence_dir / "role-homes/review/0"
    assert _native_role_home({**request, "role": "intake"}) == (
        broker.evidence_dir / "role-homes/intake/0")


@pytest.mark.parametrize("change", [
    "kind", "missing-context", "verify-role", "later-iteration", "resume-session",
    "revision-id", "namespace", "namespace-traversal", "old-plan", "old-digest",
    "proposed-plan", "proposed-digest", "context-candidate", "candidate", "candidate-base",
    "candidate-head", "outer-candidate", "outer-base", "outer-head", "hash", "path",
    "workspace", "workspace-alias", "content-rehashed", "inner-candidate-rehashed",
    "inner-revision-rehashed", "inner-proposal-rehashed", "inner-extra-rehashed", "mode",
    "hardlink", "symlink", "ancestor-symlink", "fifo",
])
def test_native_revision_preparation_refuses_changed_boundary_before_writes(native_review, change):
    broker, request = native_review
    request["review_diff"] = revision_diff(broker, request)
    artifact = Path(request["review_diff"]["path"])
    context = request["revision_context"]
    if change == "kind":
        request["review_diff"].pop("kind")
    elif change == "missing-context":
        request.pop("revision_context")
    elif change == "verify-role":
        request["role"] = "verify"
    elif change == "later-iteration":
        request["iteration"] = 1
    elif change == "resume-session":
        request["resume_session"] = "an-original-product-session"
    elif change == "revision-id":
        context["revision_id"] = "revision-" + "2" * 24
    elif change == "namespace":
        context["namespace"] = "plan-revisions/revision-" + "2" * 24
    elif change == "namespace-traversal":
        context["namespace"] = "plan-revisions/../role-homes"
    elif change == "old-plan":
        context["old_plan"]["scope"] = "Different accepted outcome"
    elif change == "old-digest":
        context["old_identity"]["plan_digest"] = "0" * 64
    elif change == "proposed-plan":
        context["proposed_plan"]["scope"] = "Different proposed outcome"
    elif change == "proposed-digest":
        context["proposed_plan_sha256"] = "0" * 64
    elif change == "context-candidate":
        context["candidate_id"] = "d" * 64
    elif change == "candidate":
        request["candidate"]["id"] = "d" * 64
    elif change == "candidate-base":
        request["candidate"]["base_sha"] = "d" * 40
    elif change == "candidate-head":
        request["candidate"]["head"] = "d" * 40
    elif change.startswith("outer-"):
        field = {"outer-candidate": "candidate_id", "outer-base": "base_sha", "outer-head": "head"}[
            change]
        request["review_diff"][field] = "d" * (64 if field == "candidate_id" else 40)
    elif change == "hash":
        request["review_diff"]["sha256"] = "0" * 64
    elif change == "path":
        request["review_diff"]["path"] = str(broker.evidence_dir / artifact.name)
    elif change == "workspace":
        request["workspace"] = request["spec"]["state_dir"]
    elif change == "workspace-alias":
        alias = broker.evidence_dir / "workspace-alias"
        alias.symlink_to(request["workspace"], target_is_directory=True)
        request["workspace"] = str(alias)
    elif change == "content-rehashed":
        artifact.write_bytes(b'{"version": 1}\n')
        request["review_diff"]["sha256"] = hashlib.sha256(artifact.read_bytes()).hexdigest()
    elif change.startswith("inner-"):
        receipt = json.loads(artifact.read_bytes())
        field = {
            "inner-candidate-rehashed": "candidate_id", "inner-revision-rehashed": "revision_id",
            "inner-proposal-rehashed": "proposed_plan_sha256", "inner-extra-rehashed": "extra",
        }[change]
        receipt[field] = "changed"
        write_private(artifact, receipt)
        request["review_diff"]["sha256"] = hashlib.sha256(artifact.read_bytes()).hexdigest()
    elif change == "mode":
        artifact.chmod(0o644)
    elif change == "hardlink":
        os.link(artifact, artifact.parent / "other-link.json")
    elif change == "symlink":
        target = artifact.with_name("substitute.json")
        artifact.rename(target)
        artifact.symlink_to(target)
    elif change == "ancestor-symlink":
        parent = artifact.parent
        target = parent.with_name("relocated-comparison")
        parent.rename(target)
        parent.symlink_to(target, target_is_directory=True)
    elif change == "fifo":
        artifact.unlink()
        os.mkfifo(artifact, 0o600)
    else:
        raise AssertionError("unimplemented boundary change")
    with pytest.raises((ValueError, OSError)):
        prepare(request)
    assert not (broker.evidence_dir / "role-homes").exists()
    assert not (broker.evidence_dir / "transient").exists()
    assert not (broker.evidence_dir / "attempts/review-attempt/auth.json").exists()


@pytest.mark.parametrize("role", ["review", "verify"])
@pytest.mark.parametrize("change", [None, "revision-kind", "revision-context", "unknown-kind",
                                    "candidate", "hash", "path", "workspace"])
def test_product_source_review_keeps_exact_patch_contract(native_review, change, role):
    broker, request = native_review
    request.pop("revision_context")
    request["role"] = role
    workspace = broker.evidence_dir / "gates/0" / role
    private_directory(workspace)
    request["workspace"] = str(workspace)
    path = broker.evidence_dir / "gate-evidence/0" / role / (request["candidate"]["id"] + ".patch")
    private_directory(path.parent)
    path.write_bytes(b"diff --git a/model.py b/model.py\n+implemented\n")
    path.chmod(0o600)
    request["review_diff"] = {
        "path": str(path), "candidate_id": request["candidate"]["id"],
        "head": request["candidate"]["head"], "base_sha": request["spec"]["base_sha"],
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    if change == "revision-kind":
        request["review_diff"]["kind"] = "plan_revision"
    elif change == "revision-context":
        request["revision_context"] = {"revision_id": "revision-" + "1" * 24}
    elif change == "unknown-kind":
        request["review_diff"]["kind"] = "untrusted"
    elif change == "candidate":
        request["review_diff"]["candidate_id"] = "d" * 64
    elif change == "hash":
        request["review_diff"]["sha256"] = "0" * 64
    elif change == "path":
        request["review_diff"]["path"] = str(broker.evidence_dir / path.name)
    elif change == "workspace":
        request["workspace"] = str(broker.evidence_dir)
    if change is not None:
        with pytest.raises(ValueError):
            prepare(request)
        assert not (broker.evidence_dir / "role-homes").exists()
    else:
        name, env = prepare(request)
        _, config = profile(env)
        assert config["permissions"][name]["filesystem"][str(path)] == "read"
        assert _native_role_home(request) == broker.evidence_dir / "role-homes" / role / "0"
        assert prepare(request) == (name, env)


def test_comparison_rejects_rehashed_old_plan_and_diff_even_with_consistent_envelope(native_review):
    broker, request = native_review
    request["review_diff"] = revision_diff(broker, request)
    request["revision_context"]["old_plan"]["scope"] = "A different accepted outcome"
    request["revision_context"]["old_identity"]["plan_digest"] = digest(
        request["revision_context"]["old_plan"])
    # Exact content comparison catches old-plan drift even when its context digest is updated.
    with pytest.raises(ValueError, match="comparison is unavailable or changed"):
        prepare(request)
    assert not (broker.evidence_dir / "role-homes").exists()
