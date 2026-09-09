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
    snapshot = provenance.capture_snapshot(repository, {
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


@pytest.mark.parametrize("source", ["instruction", "settings"])
def test_missing_captured_bytes_cannot_establish_provenance(captured, source):
    repository, snapshot, store = captured
    identity = (snapshot["instruction_sources"][0]["hash"] if source == "instruction"
                else snapshot["model_policy_hash"])
    (store.root / "artifacts" / identity).unlink()
    with pytest.raises(WorkflowError):
        provenance.validate_start_snapshot(repository, snapshot, store.require_artifact)


def test_workflow_digest_must_bind_the_captured_inputs(captured):
    repository, snapshot, store = captured
    snapshot["workflow_hash"] = "f" * 64
    with pytest.raises(WorkflowError, match="bind captured package and inputs"):
        provenance.validate_start_snapshot(repository, snapshot, store.require_artifact)
