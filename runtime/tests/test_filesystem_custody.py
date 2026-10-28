"""Explicit current-object adoption never rewrites legacy custody or a provider verdict."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal import delivery_filesystem_custody as custody
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_native_process import reconcile_process
from devflow_temporal.delivery_resources import RunResources, _identity, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def reboot(api_fixture, monkeypatch):
    path, request = api_fixture
    raw_config = json.loads(path.read_text())
    raw_config["execution_registry"] = str(path.parent / "registry.sqlite3")
    path.write_text(json.dumps(raw_config))
    config = DeliveryConfig.load(path)
    store = DeliveryStore(config)
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    spec["resource_cleanup_version"] = 1
    broker = DeliveryBroker(store, spec)
    candidate = broker.prepare()["candidate"]
    spec["provider"] = "codex"
    spec["policy"]["execution_backend"] = "native-macos"
    spec["policy_digest"] = digest(spec["policy"])
    monkeypatch.setattr(store, "effective_spec", lambda _: spec)
    monkeypatch.setattr(store, "owns_execution", lambda _: True)
    monkeypatch.setattr(
        "devflow_temporal.delivery_execution_config.load_execution_config", lambda _: config
    )
    monkeypatch.setattr(custody, "_boot", lambda: "boot-current")
    monkeypatch.setattr(custody, "_volume", lambda _: "volume-current")
    monkeypatch.setattr(
        custody,
        "_workflow",
        lambda *a: {
            "workflow_id": "delivery-run-1",
            "execution_run_id": "original-execution",
            "status": "RUNNING",
        },
    )
    monkeypatch.setattr("devflow_temporal.delivery_native_process.process_table", lambda: {})
    monkeypatch.setattr("devflow_temporal.delivery_native_process.listeners", lambda _: set())
    resources = RunResources(spec)
    resources.register(Path(spec["checkout"]), "checkout")
    resources.created(Path(spec["checkout"]))
    scratch = resources.scratch("fixture", "retained")
    journal_path = Path(spec["state_dir"]) / "gates/0/check/native/native-process.json"
    journal = {
        "intent": {
            "run_id": spec["run_id"],
            "policy_digest": spec["policy_digest"],
            "ports": [],
            "cwd": spec["checkout"],
        },
        "ports": [],
        "phase": "authorized",
        "owned": {"999981": {"identity": "old-child"}},
        "monitor": {"pid": 999982, "identity": "old-monitor"},
        "monitoring_complete": False,
        "lifecycle_locks": {},
    }
    journal_path.parent.mkdir(parents=True, mode=0o700)
    for name in ("native-monitor.lock", "native-process.lock"):
        lock = journal_path.with_name(name)
        lock.touch(mode=0o600)
        journal["lifecycle_locks"][name] = custody._raw(lock.stat())
    write_private(journal_path, journal)
    resources.process(journal_path)
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
            "state,cleanup) VALUES ('fixture-job',?,'implement',0,?,'unknown','unknown')",
            (spec["run_id"], candidate["id"]),
        )
    # Simulate ONLY mount-device renumbering in the original records; live OS paths
    # and their inodes remain real, so replacement and lock tests exercise actual FDs.
    manifest = read_private(resources.manifest)
    manifest["state_identity"]["device"] -= 1
    for entry in manifest["roots"].values():
        if entry.get("identity"):
            entry["identity"]["device"] -= 1
    write_private(resources.manifest, manifest)
    for identity in journal["lifecycle_locks"].values():
        identity["device"] -= 1
    write_private(journal_path, journal)
    originals = {p: p.read_bytes() for p in (resources.manifest, journal_path)}
    return SimpleNamespace(
        store=store,
        spec=spec,
        config=config,
        resources=resources,
        journal_path=journal_path,
        journal=journal,
        manifest=manifest,
        scratch=scratch,
        originals=originals,
    )


def command(case):
    plan = custody.preflight(case.store, case.spec["run_id"])
    return {key: plan[key] for key in ("expected_revision", "expected_evidence_sha256")} | {
        "command_id": "adopt-reboot-1",
        "answer": custody.ANSWER,
    }


def adopt(case):
    supplied = command(case)
    return supplied, custody.adopt(case.store, case.spec["run_id"], supplied)


def test_public_grant_preserves_originals_and_unlocks_exact_native_resource_readers(reboot):
    case = reboot
    with pytest.raises(ValueError, match="run resource ownership"):
        with case.resources.locked():
            pass
    assert reconcile_process(case.journal_path, spec=case.spec)["cleanup"] == "unknown"
    supplied, receipt = adopt(case)
    assert receipt["historical_volume_continuity"] == "unproven"
    assert custody.adopt(case.store, case.spec["run_id"], supplied) == receipt
    assert all(p.read_bytes() == raw for p, raw in case.originals.items())
    with case.resources.locked(read_only=True) as manifest:
        assert manifest == case.manifest
    proof = reconcile_process(case.journal_path, spec=case.spec)
    assert proof["cleanup"] == "observed-native-confirmed"
    journal = read_private(case.journal_path)
    assert journal["result"]["provider_outcome"] == "unknown"
    assert journal["monitoring_complete"] is False
    assert journal["lifecycle_locks"] == case.journal["lifecycle_locks"]
    # Canonical monitoring records may advance; the archived originals remain exact.
    assert custody.matches(
        case.spec,
        case.journal_path.with_name("native-process.lock"),
        case.journal["lifecycle_locks"]["native-process.lock"],
        directory=False,
    )
    assert custody.matches(
        case.spec,
        Path(case.spec["checkout"]),
        case.manifest["roots"][case.spec["checkout"]]["identity"],
    )
    assert (
        _identity(Path(case.spec["checkout"]))
        != case.manifest["roots"][case.spec["checkout"]]["identity"]
    )  # New allocations still use the real OS device.


@pytest.mark.parametrize("drift", ["revision", "source", "journal", "manifest"])
def test_preflight_is_observation_and_stale_evidence_never_grants_authority(reboot, drift):
    case = reboot
    from devflow_temporal.delivery_lifecycle import RecoveryRefused

    preview = custody.preflight(case.store, case.spec["run_id"])
    assert preview["lifecycle_assessment"]["blockers"][0]["code"] == (
        "filesystem_adoption_authorization_required")
    supplied = command(case)
    if drift == "revision":
        with case.store._connect() as db:
            db.execute("UPDATE delivery_runs SET revision=revision+1")
    elif drift == "source":
        Path(case.spec["checkout"]).joinpath("README.md").write_text("changed after preflight")
    elif drift == "journal":
        journal = read_private(case.journal_path)
        journal["intent"]["policy_digest"] = "changed"
        write_private(case.journal_path, journal)
    else:
        manifest = read_private(case.resources.manifest)
        manifest["roots"][case.spec["checkout"]]["generation"] = 99
        write_private(case.resources.manifest, manifest)
    with pytest.raises(ValueError) as failure:
        custody.adopt(case.store, case.spec["run_id"], supplied)
    if drift == "revision":
        assert isinstance(failure.value, RecoveryRefused)
        assert failure.value.assessment["blockers"][0]["code"] == "revision_changed"
    with case.store._connect() as db:
        assert (
            db.execute("SELECT 1 FROM delivery_events WHERE type=?", (custody.KIND,)).fetchone()
            is None
        )


@pytest.mark.parametrize("table", ["delivery_effects", "delivery_mutations"])
def test_effect_or_cancel_inserted_without_run_revision_change_is_fenced(
    reboot, monkeypatch, table
):
    case = reboot
    supplied = command(case)
    collect = custody._collect

    def race(*args):
        result = collect(*args)
        with case.store._connect() as db:
            if table == "delivery_effects":
                db.execute(
                    "INSERT INTO delivery_effects "
                    "(effect_key,run_id,kind,state,request_json,updated_at) "
                    "VALUES ('concurrent-publish',?,'publish','pending','{}','now')",
                    (case.spec["run_id"],),
                )
            else:
                db.execute(
                    "INSERT INTO delivery_mutations (command_id,run_id,kind,request_digest,"
                    "state) VALUES ('concurrent-cancel',?,'cancel','digest','pending')",
                    (case.spec["run_id"],),
                )
        return result

    monkeypatch.setattr(custody, "_collect", race)
    with pytest.raises(ValueError, match="concurrent uncertain"):
        custody.adopt(case.store, case.spec["run_id"], supplied)


def test_staged_receipt_is_inert_and_exact_retry_recovers_after_acknowledgement_loss(
    reboot, monkeypatch
):
    case = reboot
    supplied = command(case)
    event = case.store._event

    def lost(*args, **kwargs):
        raise RuntimeError("lost after receipt fsync")

    monkeypatch.setattr(case.store, "_event", lost)
    with pytest.raises(RuntimeError, match="receipt fsync"):
        custody.adopt(case.store, case.spec["run_id"], supplied)
    assert not custody.matches(
        case.spec, Path(case.spec["state_dir"]), case.manifest["state_identity"]
    )
    monkeypatch.setattr(case.store, "_event", event)
    receipt = custody.adopt(case.store, case.spec["run_id"], supplied)
    assert custody.adopt(case.store, case.spec["run_id"], supplied) == receipt
    assert custody.matches(case.spec, Path(case.spec["state_dir"]), case.manifest["state_identity"])


def test_process_loss_after_atomic_publication_leaves_single_link_and_exact_retry(tmp_path):
    path = tmp_path / "archive/original.json"
    script = """
import os, sys
from pathlib import Path
from devflow_temporal import delivery_filesystem_custody as custody
publish = custody._publish_exclusive

def lose_after_publish(stage, path):
    publish(stage, path)
    os._exit(71)

custody._publish_exclusive = lose_after_publish
custody._immutable(Path(sys.argv[1]), b'{"complete": true}\\n')
"""
    result = subprocess.run([sys.executable, "-I", "-c", script, str(path)], timeout=10)
    assert result.returncode == 71
    assert path.stat().st_nlink == 1
    raw = b'{"complete": true}\n'
    assert path.read_bytes() == raw
    custody._immutable(path, raw)
    assert read_private(path, raw=True) == raw
    assert not list(path.parent.glob(".custody-*"))


def test_actual_cli_preflight_bootstrap_never_constructs_a_writable_store(
    reboot,
    monkeypatch,
    capsys,
):
    from devflow_temporal import delivery_control, delivery_store

    case = reboot
    before = case.config.tracking_db.read_bytes()
    monkeypatch.setattr(
        delivery_store,
        "_private_directory",
        lambda *a: pytest.fail("writable store initialization"),
    )
    monkeypatch.setattr(
        delivery_store,
        "preserve_execution_config",
        lambda *a: pytest.fail("configuration publication"),
    )

    def observe(reader, run_id):
        assert reader.read_only is True
        with reader._connect() as db:
            assert db.execute("PRAGMA query_only").fetchone()[0] == 1
            assert db.execute("SELECT run_id FROM delivery_runs").fetchone()[0] == run_id
        return {"run_id": run_id, "observed": True}

    monkeypatch.setattr(custody, "preflight", observe)
    monkeypatch.setattr(
        "sys.argv",
        [
            "devflow-delivery",
            "--config",
            str(case.config.path),
            "filesystem-custody-preflight",
            "--id",
            case.spec["run_id"],
        ],
    )
    delivery_control.main()
    assert json.loads(capsys.readouterr().out)["observed"] is True
    assert case.config.tracking_db.read_bytes() == before


@pytest.mark.parametrize("removed_state", ["removed", "already_absent"])
def test_an_acknowledged_grant_cannot_extend_a_resource_after_its_custody_ends(
    reboot, removed_state
):
    case = reboot
    adopt(case)
    root = Path(case.spec["state_dir"]) / "transient"
    manifest = read_private(case.resources.manifest)
    entry = manifest["roots"][str(root)]
    old_identity = copy.deepcopy(entry["identity"])
    entry["state"] = "removed"
    entry["receipt"] = {"path": str(root), "kind": entry["kind"], "state": removed_state}
    write_private(case.resources.manifest, manifest)
    assert not custody.matches(case.spec, root, old_identity)
