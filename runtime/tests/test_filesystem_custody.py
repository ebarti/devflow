"""Explicit current-object adoption never rewrites legacy custody or a provider verdict."""

from __future__ import annotations

import copy
import fcntl
import json
import os
import sqlite3
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
    with pytest.raises(ValueError):
        custody.adopt(case.store, case.spec["run_id"], supplied)
    with case.store._connect() as db:
        assert (
            db.execute("SELECT 1 FROM delivery_events WHERE type=?", (custody.KIND,)).fetchone()
            is None
        )


@pytest.mark.parametrize(
    "drift", ["replacement", "symlink", "unproven", "held", "listener", "live", "running-attempt"]
)
def test_adoption_refuses_unproven_objects_and_live_custody(reboot, monkeypatch, drift):
    case = reboot
    held = None
    lock = case.journal_path.with_name("native-monitor.lock")
    if drift == "replacement":
        lock.rename(lock.with_suffix(".old"))
        lock.touch(mode=0o600)
    elif drift == "symlink":
        lock.rename(lock.with_suffix(".old"))
        lock.symlink_to(lock.with_suffix(".old"))
    elif drift == "unproven":
        manifest = read_private(case.resources.manifest)
        manifest["roots"][case.spec["checkout"]]["identity"] = None
        write_private(case.resources.manifest, manifest)
    elif drift == "held":
        held = os.open(lock, os.O_RDWR)
        fcntl.flock(held, fcntl.LOCK_EX)
    elif drift == "listener":
        journal = read_private(case.journal_path)
        journal["ports"] = journal["intent"]["ports"] = [19999]
        write_private(case.journal_path, journal)
        monkeypatch.setattr("devflow_temporal.delivery_native_process.listeners", lambda _: {1234})
    elif drift == "live":
        monkeypatch.setattr(
            "devflow_temporal.delivery_native_process.process_table",
            lambda: {999981: {"identity": "old-child", "stat": "S"}},
        )
    else:
        with case.store._connect() as db:
            db.execute("UPDATE delivery_attempts SET state='running'")
    try:
        with pytest.raises((ValueError, OSError)):
            custody.preflight(case.store, case.spec["run_id"])
    finally:
        if held is not None:
            os.close(held)


def test_pid_reuse_in_a_later_journal_does_not_hide_an_earlier_live_actor(reboot, monkeypatch):
    case = reboot
    later = case.journal_path.parent.parent / "later/native-process.json"
    later.parent.mkdir(parents=True)
    journal = copy.deepcopy(case.journal)
    journal["owned"]["999981"]["identity"] = "different-start-identity"
    for name in ("native-monitor.lock", "native-process.lock"):
        lock = later.with_name(name)
        lock.touch(mode=0o600)
        journal["lifecycle_locks"][name] = custody._raw(lock.stat()) | {
            "device": lock.stat().st_dev - 1
        }
    write_private(later, journal)
    manifest = read_private(case.resources.manifest)
    manifest["processes"].append(str(later))
    write_private(case.resources.manifest, manifest)
    monkeypatch.setattr(
        "devflow_temporal.delivery_native_process.process_table",
        lambda: {999981: {"identity": "old-child", "stat": "S"}},
    )
    with pytest.raises(ValueError, match="live original native actor"):
        custody.preflight(case.store, case.spec["run_id"])


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


@pytest.mark.parametrize("drift", ["boot", "volume", "receipt", "archive", "generation"])
def test_acknowledged_grant_is_exact_and_new_generations_do_not_inherit_it(
    reboot, monkeypatch, drift
):
    case = reboot
    _, receipt = adopt(case)
    checkout = Path(case.spec["checkout"])
    old = case.manifest["roots"][str(checkout)]["identity"]
    if drift == "boot":
        monkeypatch.setattr(custody, "_boot", lambda: "another-boot")
    elif drift == "volume":
        monkeypatch.setattr(custody, "_volume", lambda _: "another-volume")
    elif drift == "receipt":
        path = Path(receipt["receipt"])
        path.write_text("{}")
    elif drift == "archive":
        path = Path(receipt["receipt"])
        payload = json.loads(path.read_text())
        sha = payload["evidence"]["originals"][str(case.resources.manifest)]
        path.parent.joinpath("originals", sha + ".json").write_text("{}")
    else:
        with case.resources.locked() as manifest:
            manifest["roots"][str(checkout)]["generation"] = 1
            write_private(case.resources.manifest, manifest)
        assert not custody.matches(case.spec, checkout, old)
        return
    with pytest.raises((ValueError, KeyError)):
        custody.matches(case.spec, checkout, old)


@pytest.mark.parametrize("kind", ["file", "symlink"])
def test_normal_resource_identity_checks_keep_directory_and_owner_semantics(tmp_path, kind):
    path = tmp_path / "resource"
    path.write_text("not a directory")
    if kind == "symlink":
        target = tmp_path / "target"
        path.rename(target)
        path.symlink_to(target)
    identity = custody._raw(path.lstat())
    with pytest.raises(ValueError, match="type or ownership"):
        custody.matches(None, path, identity)


def test_adopted_identity_cannot_bypass_boot_or_volume_fence_when_old_device_is_reused(
    reboot,
    monkeypatch,
):
    case = reboot
    adopt(case)
    checkout = Path(case.spec["checkout"])
    old = case.manifest["roots"][str(checkout)]["identity"]
    physical = custody._raw
    monkeypatch.setattr(
        custody, "_raw", lambda info: old if info.st_ino == old["inode"] else physical(info)
    )
    monkeypatch.setattr(custody, "_boot", lambda: "another-boot")
    with pytest.raises(ValueError, match="adopted filesystem object identity changed"):
        custody.matches(case.spec, checkout, old)


def test_incomplete_archive_publication_has_no_final_path_and_retry_is_safe(tmp_path, monkeypatch):
    path = tmp_path / "archive/original.json"
    publish = custody._publish_exclusive
    monkeypatch.setattr(
        custody,
        "_publish_exclusive",
        lambda *a, **k: (_ for _ in ()).throw(OSError("lost before atomic publication")),
    )
    with pytest.raises(OSError, match="atomic publication"):
        custody._immutable(path, b'{"complete": true}\n')
    assert not path.exists()
    monkeypatch.setattr(custody, "_publish_exclusive", publish)
    custody._immutable(path, b'{"complete": true}\n')
    assert path.read_bytes() == b'{"complete": true}\n'


def test_public_cli_binds_preflight_and_explicit_grant_without_starting_a_service(
    reboot,
    monkeypatch,
    capsys,
    tmp_path,
):
    from devflow_temporal import delivery_control

    case = reboot
    monkeypatch.setattr(delivery_control, "DeliveryStore", lambda *a, **kw: case.store)
    monkeypatch.setattr(delivery_control, "service_start", lambda *a, **kw: pytest.fail("startup"))
    common = ["devflow-delivery", "--config", str(case.config.path)]
    monkeypatch.setattr("sys.argv", common + ["filesystem-custody-preflight", "--id", "run-1"])
    delivery_control.main()
    plan = json.loads(capsys.readouterr().out)
    supplied = {k: plan[k] for k in ("expected_revision", "expected_evidence_sha256")} | {
        "command_id": "cli-adoption",
        "answer": custody.ANSWER,
    }
    request = tmp_path / "grant.json"
    request.write_text(json.dumps(supplied))
    with case.store._connect() as db:
        attempts = [dict(row) for row in db.execute("SELECT * FROM delivery_attempts")]
    monkeypatch.setattr(
        "sys.argv",
        common + ["adopt-filesystem-custody", "--id", "run-1", "--request", str(request)],
    )
    delivery_control.main()
    result = json.loads(capsys.readouterr().out)
    assert result["kind"] == custody.KIND
    with case.store._connect() as db:
        assert [dict(row) for row in db.execute("SELECT * FROM delivery_attempts")] == attempts
    assert all(p.read_bytes() == raw for p, raw in case.originals.items())


def test_ordinary_finalization_reuses_grant_without_widening_deletion_or_passing_a_gate(reboot):
    case = reboot
    adopt(case)
    result = case.resources.finalize("blocked")
    assert result["process_cleanup"] == "observed-native-confirmed"
    assert result["resource_cleanup"] == "confirmed"
    assert not case.scratch.exists()
    assert Path(case.spec["checkout"]).exists()  # Blocked recovery source is deliberately retained.
    assert read_private(case.journal_path)["result"]["provider_outcome"] == "unknown"


def test_public_preflight_store_cannot_publish_configuration_or_write_database(reboot, monkeypatch):
    from devflow_temporal import delivery_store

    case = reboot
    before = case.config.tracking_db.read_bytes()
    monkeypatch.setattr(
        delivery_store,
        "preserve_execution_config",
        lambda *a: pytest.fail("read-only snapshot publication"),
    )
    reader = DeliveryStore(case.config, read_only=True)
    with reader._connect() as db:
        assert db.execute("SELECT run_id FROM delivery_runs").fetchone()[0] == case.spec["run_id"]
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.execute("UPDATE delivery_runs SET phase='blocked'")
    assert case.config.tracking_db.read_bytes() == before


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
