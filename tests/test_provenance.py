import shutil
from pathlib import Path

import pytest

from devflow import provenance
from devflow.adapters.sqlite_store import SQLiteStore
from devflow.errors import WorkflowError


@pytest.fixture
def captured(tmp_path, monkeypatch):
    repository = tmp_path / "repository"
    shutil.copytree(Path(__file__).parents[1] / "fixtures/repositories/prose", repository)
    # Release authenticity has its own real installed-CLI acceptance proof.
    monkeypatch.setattr(provenance, "installed_release", lambda _: {"revision": "a" * 40})
    store = SQLiteStore(tmp_path / "private")
    policy = repository / "model-policy.md"
    policy.write_text("Use configured defaults; keep explicit role overrides.\n")
    snapshot = provenance.capture_snapshot(repository, {
        "model_policy_paths": [str(policy)],
        "snapshot_id": "synthetic-snapshot", "effective_settings": {
            "model": "synthetic-model", "reasoning_effort": "synthetic-effort",
            "service_tier": "synthetic-tier", "source_reference": "synthetic:observed-settings",
        },
    }, store.put_artifact)
    return repository, snapshot, store


def test_captured_snapshot_admits_actual_stored_inputs(captured):
    repository, snapshot, store = captured
    provenance.validate_start_snapshot(repository, snapshot, store.require_artifact)


def test_missing_revision_rejected_before_claim_admission(captured):
    repository, snapshot, store = captured
    del snapshot["package_revision"]
    with pytest.raises(WorkflowError, match="package_revision"):
        provenance.validate_start_snapshot(repository, snapshot, store.require_artifact)


def test_different_revision_cannot_pin_new_attempt_to_another_runtime(captured):
    repository, snapshot, store = captured
    snapshot["package_revision"] = "b" * 40
    with pytest.raises(WorkflowError, match="executing pinned release"):
        provenance.validate_start_snapshot(repository, snapshot, store.require_artifact)


@pytest.mark.parametrize("source", ["instruction", "settings", "policy", "policy-source"])
def test_missing_captured_bytes_cannot_establish_provenance(captured, source):
    repository, snapshot, store = captured
    identity = {
        "instruction": snapshot["instruction_sources"][0]["hash"],
        "settings": snapshot["effective_settings_reference"].removeprefix("sha256:"),
        "policy": snapshot["model_policy_hash"],
        "policy-source": snapshot["model_policy_sources"][0]["hash"],
    }[source]
    (store.root / "artifacts" / identity).unlink()
    with pytest.raises(WorkflowError):
        provenance.validate_start_snapshot(repository, snapshot, store.require_artifact)


def test_workflow_digest_must_bind_the_captured_inputs(captured):
    repository, snapshot, store = captured
    snapshot["workflow_hash"] = "f" * 64
    with pytest.raises(WorkflowError, match="bind captured package and inputs"):
        provenance.validate_start_snapshot(repository, snapshot, store.require_artifact)


def test_actual_model_change_does_not_rewrite_routing_policy(captured):
    repository, before, store = captured
    request = {
        "snapshot_id": "different-execution", "model_policy_paths": [str(repository / "model-policy.md")],
        "effective_settings": {"model": "another-observed-model", "reasoning_effort": "high",
                               "service_tier": "default", "source_reference": "native:next-turn"},
    }
    after = provenance.capture_snapshot(repository, request, store.put_artifact)
    assert before["model_policy_hash"] == after["model_policy_hash"]
    assert before["workflow_hash"] == after["workflow_hash"]
    assert before["effective_settings_reference"] != after["effective_settings_reference"]
    assert after["model_policy_reference"] != after["effective_settings_reference"]
    provenance.validate_start_snapshot(repository, after, store.require_artifact)


def test_routing_change_preserves_actual_settings_and_updates_policy(captured):
    repository, before, store = captured
    settings = __import__("json").loads(
        (store.root / "artifacts" / before["effective_settings_reference"].removeprefix("sha256:")).read_text()
    )
    (repository / "model-policy.md").write_text("Review uses an explicitly selected override.\n")
    after = provenance.capture_snapshot(repository, {
        "snapshot_id": "changed-policy", "effective_settings": settings,
        "model_policy_paths": [str(repository / "model-policy.md")],
    }, store.put_artifact)
    assert before["effective_settings_reference"] == after["effective_settings_reference"]
    assert before["model_policy_hash"] != after["model_policy_hash"]
    assert before["workflow_hash"] != after["workflow_hash"]
    provenance.validate_start_snapshot(repository, after, store.require_artifact)


def test_absent_routing_policy_is_unknown_instead_of_actual_settings(captured):
    repository, _, store = captured
    snapshot = provenance.capture_snapshot(repository, {
        "snapshot_id": "unobserved-policy", "effective_settings": {
            "model": "observed-model", "reasoning_effort": "high", "service_tier": "unknown",
            "source_reference": "native:observed-turn",
        },
    }, store.put_artifact)
    assert snapshot["model_policy_status"] == "unavailable"
    assert snapshot["model_policy_hash"] is None
    assert snapshot["model_policy_reference"] is None
    assert snapshot["model_policy_sources"] == []
    assert snapshot["effective_settings_reference"].startswith("sha256:")
    provenance.validate_start_snapshot(repository, snapshot, store.require_artifact)


def test_policy_manifest_cannot_be_replaced_by_settings_hash(captured):
    repository, snapshot, store = captured
    snapshot["model_policy_hash"] = snapshot["effective_settings_reference"].removeprefix("sha256:")
    snapshot["model_policy_reference"] = snapshot["effective_settings_reference"]
    with pytest.raises(WorkflowError, match="Routing policy"):
        provenance.validate_start_snapshot(repository, snapshot, store.require_artifact)
