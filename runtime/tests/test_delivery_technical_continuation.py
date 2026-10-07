from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import subprocess
import sys
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio import workflow
from temporalio.exceptions import ApplicationError
from test_delivery_gates_admission import stopped as stopped
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_native_renewal import payload_update as payload_update
from test_delivery_resources import spec as resource_spec
from test_delivery_store import service as service
from test_delivery_store import submit_historical_admission

from devflow_temporal import delivery_native_renewal as renewal
from devflow_temporal import delivery_technical_continuation as technical
from devflow_temporal import delivery_technical_integration as integration
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker, _git
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.mark.parametrize("size", [1358726, 1894818])
def test_complete_authentic_row_sizes_have_private_bounded_readback(tmp_path, size):
    empty = json.dumps({"run_id": "run-1", "padding": ""}, separators=(",", ":")).encode()
    raw = json.dumps(
        {"run_id": "run-1", "padding": "x" * (size - len(empty))}, separators=(",", ":")
    ).encode()
    path = tmp_path / "sealed-row.json"
    path.write_bytes(raw)
    path.chmod(0o600)
    binding = {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}
    assert len(raw) == size
    assert technical._sealed_row(binding, "run-1")["run_id"] == "run-1"
    with pytest.raises(ValueError, match="unsafe"):
        integration.reference(str(path), binding["sha256"])
    assert path.read_bytes() == raw and path.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "adverse",
    ["size", "hash", "alias", "parent-alias", "hardlink", "mode", "owner", "foreign-run", "type"],
)
def test_sealed_row_refuses_oversize_alias_hash_foreign_or_nonprivate_before_effects(
    tmp_path,
    monkeypatch,
    adverse,
):
    value = (
        ["run-1"]
        if adverse == "type"
        else {"run_id": "other" if adverse == "foreign-run" else "run-1", "payload": "retained"}
    )
    raw = json.dumps(value).encode()
    if adverse == "size":
        raw = b" " * (technical.SEALED_ROW_LIMIT + 1)
    root = tmp_path / "rows"
    root.mkdir()
    path = root / "sealed.json"
    path.write_bytes(raw)
    path.chmod(0o600)
    binding = {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}
    if adverse == "hash":
        binding["sha256"] = "f" * 64
    elif adverse in {"alias", "parent-alias"}:
        alias = tmp_path / "alias"
        alias.symlink_to(path if adverse == "alias" else root)
        binding["path"] = str(alias if adverse == "alias" else alias / path.name)
    elif adverse == "hardlink":
        os.link(path, root / "other")
    elif adverse == "mode":
        path.chmod(0o644)
    elif adverse == "owner":
        monkeypatch.setattr(technical.os, "getuid", lambda: path.stat().st_uid + 1)
    before = path.read_bytes()
    with pytest.raises(ValueError):
        technical._sealed_row(binding, "run-1")
    assert path.read_bytes() == before


def request():
    return {
        "continuation_kind": technical.KIND,
        "command_id": "technical-1",
        "expected_revision": 27,
        "expected_iteration": 4,
        "expected_candidate_id": "a" * 64,
        "expected_pr_number": 1047,
        "expected_pr_head": "b" * 40,
        "additional_iterations": 0,
        "authority_path": "/absent/authority.json",
        "authority_sha256": "c" * 64,
        "expected_source_revision": "d" * 40,
    }


@pytest.fixture
def accepted_gate_boundary(stopped, monkeypatch):
    """Production admission/Git/resources; only native installation is a controlled seam."""
    store, broker, old_closed, gate_command = stopped
    store.admit_gates_only("run-1", gate_command)
    _git(broker.checkout, "add", ".")
    _git(broker.checkout, "commit", "-qm", "feat: preserve accepted source")
    _git(broker.checkout, "push", "origin", broker.spec["branch"])
    candidate = broker.candidate()
    publication = {
        "number": 7,
        "url": "https://example.invalid/pull/7",
        "state": "OPEN",
        "head": candidate["head"],
        "base": broker.spec["base_sha"],
        "candidate": candidate,
    }
    broker._effect("publish:run-1:4", "publish", {"iteration": 4})
    broker._finish_effect("publish:run-1:4", publication)
    monkeypatch.setattr(
        DeliveryBroker,
        "_existing_pr",
        lambda *_a, **_kw: {
            "number": 7,
            "url": publication["url"],
            "headRefOid": candidate["head"],
            "state": "OPEN",
            "isDraft": False,
        },
    )
    failed = {
        "role": "review",
        "iteration": 4,
        "status": "blocked",
        "finish_reason": "prelaunch",
        "session_id": None,
        "usage": None,
        "cleanup": "confirmed",
        "candidate": candidate,
    }
    with store._connect() as db:
        previous = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
        db.execute(
            "INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
            "state,result_json,cleanup) VALUES ('failed-review','run-1','review',4,?,"
            "'finished',?,'confirmed')",
            (candidate["id"], canonical_json(failed)),
        )
        store.state.release_work(db, "work-1", "external:devflow:run-1")
    resources = technical.RunResources(broker.spec)
    cleanup = resources.finalize("blocked")
    state = {
        **old_closed["result"],
        "candidate": candidate,
        "pull_request": publication,
        "roles": [*old_closed["result"]["roles"], failed],
        "revision": 27,
        "checks": {
            "prepublish": {"state": "passed", "cleanup": "confirmed"},
            "resource_cleanup": cleanup,
        },
        "cleanup": "confirmed",
        "error": "repair limit exhausted",
    }
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message=state["error"],
        candidate=candidate,
        pull_request=publication,
        checks=state["checks"],
        iteration=4,
        protocol_revision=27,
        outcome="blocked",
        cleanup="confirmed",
        error=state["error"],
    )
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    closed = {
        **old_closed,
        "workflow_id": row["workflow_id"],
        "recovery_digest": digest(previous),
        "result": state,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_kw: closed)
    root = store.config.state_root

    def retain(name, value):
        path = root / name
        write_private(path, value)
        return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    row_reference = retain("stopped-row.json", row)
    final = Path(cleanup["receipt"])
    manifest = final.with_name("manifest.json")
    failure = retain(
        "technical-failure.json",
        {
            "runs": {
                "run-1": {
                    "row": row_reference,
                    "resources": {
                        "resources/manifest.json": {
                            "sha256": hashlib.sha256(manifest.read_bytes()).hexdigest()
                        },
                        "resources/finalization.json": {
                            "sha256": hashlib.sha256(final.read_bytes()).hexdigest()
                        },
                    },
                }
            }
        },
    )
    marker = retain("controlled-native-prerequisite.json", {"fixture": True})
    authority = {
        "decision_owner": "main task",
        "new_user_approval_required": False,
        "authority_source": "Controlled public custody regression; no native success claim",
        "technical_limits": {
            "max_successor_commands_per_run": 1,
            "max_additional_native_preparation_generations_per_run": 1,
            "max_total_additional_native_preparation_generations": 2,
            "max_owned_probe_attempts_per_additional_generation": 2,
            "native_renewal_provider_turns": 0,
            "native_renewal_implementation_turns": 0,
            "additional_feature_repair_grants": 0,
            "907_implementation_turns": 0,
            "907_iteration_ceiling": 4,
            "1005_iteration_ceiling": 5,
        },
        "907": {
            "run_id": "run-1",
            "work_id": "work-1",
            "frozen_base": broker.spec["base_sha"],
            "published_head": candidate["head"],
            "published_pr": publication["url"],
            "original_implementer_session": "original-session",
            "allowed_source_change": False,
        },
        "1005_integration": {"run_id": "other"},
        "trigger_bindings": {
            "sealed_actual_failures": failure,
            "consumed_native_renewal_authority": marker,
            "consumed907_gates_only_authority": {
                "path": gate_command["authority_path"],
                "sha256": gate_command["authority_sha256"],
            },
            "consumed1005_metadata_authority": marker,
            "1005_conflict_classification": marker,
            "1005_three_tree_inputs": marker,
        },
    }
    authority_reference = retain("technical-authority.json", authority)
    payload = {
        **request(),
        "expected_candidate_id": candidate["id"],
        "expected_pr_number": 7,
        "expected_pr_head": candidate["head"],
        "authority_path": authority_reference["path"],
        "authority_sha256": authority_reference["sha256"],
    }
    monkeypatch.setattr(
        technical, "_source_readiness", lambda *_a: {"controlled_native_seam": True}
    )
    return store, broker, payload


@pytest.mark.skipif(sys.platform != "darwin", reason="actual owned macOS root observation")
@pytest.mark.parametrize(
    "adverse", [None, "command", "authority", "source", "closed-history", "proof", "controller"]
)
def test_orphan_exclusive_intent_recovers_same_bytes_after_transaction_and_actor_loss(
    accepted_gate_boundary,
    monkeypatch,
    adverse,
):
    store, broker, payload = accepted_gate_boundary
    root = broker.state_dir / "technical-successor"
    original = technical._immutable
    actor = {
        "pid": 111,
        "identity": "original birth",
        "source_revision": payload["expected_source_revision"],
    }
    monkeypatch.setattr(technical, "_controller", lambda _payload: dict(actor))

    def lost_commit(path, *args, **kwargs):
        original(path, *args, **kwargs)
        if path.name == "intent.json":
            raise RuntimeError("lost after exclusive intent before SQLite commit")

    monkeypatch.setattr(technical, "_immutable", lost_commit)
    with pytest.raises(RuntimeError, match="exclusive intent"):
        store.continue_repair("run-1", payload)
    before = (root / "intent.json").read_bytes()
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_technical_successors").fetchone()[0] == 0
        assert store.state.claim_for(db, "work-1") is None
    assert not (root / "predecessor-resources").exists()
    actor.update(pid=222, identity="fresh birth")
    monkeypatch.setattr(technical, "_immutable", original)
    proposed = dict(payload)
    if adverse == "command":
        proposed["command_id"] = "foreign-command"
    elif adverse == "authority":
        proposed["authority_sha256"] = "f" * 64
    elif adverse == "source":
        (broker.checkout / "README.md").write_text("unaccepted source drift")
    elif adverse == "closed-history":
        closed = store._completed_temporal_result("run-1")
        monkeypatch.setattr(
            store,
            "_completed_temporal_result",
            lambda *_a, **_kw: {**closed, "request_digest": "f" * 64},
        )
    elif adverse == "proof":
        monkeypatch.setattr(
            technical,
            "_source_readiness",
            lambda *_a: (_ for _ in ()).throw(ValueError("consumed native proof drift")),
        )
    elif adverse == "controller":
        forged = read_private(root / "intent.json")
        forged["controller"]["source_revision"] = "e" * 40
        write_private(root / "intent.json", forged)
        before = (root / "intent.json").read_bytes()
    if adverse:
        for operation in (store.repair_admission_preflight, store.continue_repair):
            with pytest.raises(ValueError):
                operation("run-1", proposed)
        with store._connect() as db:
            assert (
                db.execute("SELECT COUNT(*) FROM delivery_technical_successors").fetchone()[0] == 0
            )
            assert store.state.claim_for(db, "work-1") is None
        assert not (root / "predecessor-resources").exists()
    else:
        observed = store.repair_admission_preflight("run-1", payload)
        assert observed["preflight"] and observed["additional_iterations"] == 0
        monkeypatch.setattr(
            renewal,
            "renew",
            lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("next native boundary")),
        )
        for _ in range(2):
            with pytest.raises(RuntimeError, match="next native"):
                store.continue_repair("run-1", payload)
        resumes = list((root / "resume-actors").glob("*.json"))
        assert len(resumes) == 1
        recorded = read_private(resumes[0])
        assert recorded["original_controller"]["pid"] == 111
        assert recorded["observed_controller"] == actor
        with store._connect() as db:
            assert (
                db.execute("SELECT COUNT(*) FROM delivery_technical_successors").fetchone()[0] == 1
            )
            assert store.state.claim_for(db, "work-1") is None
            assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
    assert (root / "intent.json").read_bytes() == before


@pytest.mark.skipif(sys.platform != "darwin", reason="actual owned macOS root observation")
def test_corrected_same_request_preflight_and_pending_interruption_keep_history_and_release_claim(
    accepted_gate_boundary,
    monkeypatch,
):
    store, broker, payload = accepted_gate_boundary
    root = broker.state_dir / "technical-successor"
    for invalid in (
        {k: v for k, v in payload.items() if k != "authority_path"},
        {**payload, "authority_sha256": "f" * 64},
    ):
        with pytest.raises(ValueError):
            store.continue_repair("run-1", invalid)
        assert not root.exists()
    preflight = store.repair_admission_preflight("run-1", payload)
    assert preflight["additional_iterations"] == 0 and preflight["resume_stage"] == "review"
    assert not root.exists()
    predecessor = {
        name: (broker.state_dir / "resources" / name).read_bytes()
        for name in ("manifest.json", "finalization.json")
    }
    calls = []

    def interrupted(*_a, **_kw):
        calls.append("native-child")
        raise RuntimeError("controlled child preparation interruption")

    monkeypatch.setattr(renewal, "renew", interrupted)
    with pytest.raises(RuntimeError, match="interruption"):
        store.continue_repair("run-1", payload)
    assert len(calls) == 1
    assert read_private(root / "closure.json")["state"] == "confirmed"
    for name, raw in predecessor.items():
        assert (root / "predecessor-resources" / name).read_bytes() == raw
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") is None
        assert db.execute("SELECT COUNT(*) FROM delivery_technical_successors").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
    monkeypatch.setattr(
        technical, "_snapshot", lambda *_a: pytest.fail("accepted intent must not be recaptured")
    )
    with pytest.raises(RuntimeError, match="interruption"):
        store.continue_repair("run-1", payload)
    assert len(calls) == 2
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") is None
    with pytest.raises(ValueError, match="one technical successor"):
        store.continue_repair("run-1", {**payload, "command_id": "other"})


@pytest.mark.skipif(sys.platform != "darwin", reason="actual owned macOS root observation")
def test_lost_closure_response_reads_retained_cleanup_without_rewriting_history(
    accepted_gate_boundary,
    monkeypatch,
):
    store, broker, payload = accepted_gate_boundary
    root = broker.state_dir / "technical-successor"
    original = technical._immutable

    def lost_response(path, *args, **kwargs):
        original(path, *args, **kwargs)
        if path.name == "closure-finalization.json":
            raise RuntimeError("lost retained closure response")

    monkeypatch.setattr(technical, "_immutable", lost_response)
    with pytest.raises(RuntimeError, match="lost retained"):
        store.continue_repair("run-1", payload)
    retained = (root / "closure-finalization.json").read_bytes()
    monkeypatch.setattr(technical, "_immutable", original)
    monkeypatch.setattr(
        technical.RunResources,
        "finalize",
        lambda *_a, **_kw: pytest.fail("completed closure must be read without finalizing again"),
    )
    monkeypatch.setattr(
        renewal,
        "renew",
        lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("next native boundary")),
    )
    with pytest.raises(RuntimeError, match="next native"):
        store.continue_repair("run-1", payload)
    assert (root / "closure-finalization.json").read_bytes() == retained
    assert read_private(root / "closure.json")["state"] == "confirmed"
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") is None


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native process/lease observation")
@pytest.mark.parametrize("adverse", [None, "unknown", "lease", "actor-inventory", "journal-alias"])
def test_unknown_closure_observes_real_closed_native_actor_without_normalizing_history(
    tmp_path,
    adverse,
):
    from devflow_temporal.delivery_native_process import NativeProcess

    spec = resource_spec(tmp_path)
    registry = technical.RunResources(spec)
    scratch = registry.scratch("check", "retained")
    actor = NativeProcess(
        spec,
        Path(spec["state_dir"]) / "check-actor",
        argv=[sys.executable, "-c", "print('owned check')"],
        cwd=scratch,
        environment={"PATH": "/usr/bin:/bin"},
        timeout=5,
    )
    assert actor.run()["cleanup"] == "observed-native-confirmed"
    receipt = registry.finalize("blocked", uncertain=True)
    assert receipt["state"] == "unknown"
    before = {p.name: p.read_bytes() for p in (registry.manifest, Path(receipt["receipt"]))}
    lock = None
    try:
        if adverse == "lease":
            lock = (actor.folder / "native-process.lock").open("rb")
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif adverse == "actor-inventory":
            value = read_private(Path(receipt["receipt"]))
            value["processes"][0]["observed_pids"] = []
            write_private(Path(receipt["receipt"]), value)
        elif adverse == "journal-alias":
            original = actor.journal.with_name("original.json")
            actor.journal.rename(original)
            actor.journal.symlink_to(original)
        if adverse:
            with pytest.raises(ValueError):
                technical._observe_resources(spec, unknown_allowed=adverse != "unknown")
        else:
            observed = technical._observe_resources(spec, unknown_allowed=True)
            assert len(observed["journal_sha256"]) == 1 and scratch.exists()
            assert {
                p.name: p.read_bytes() for p in (registry.manifest, Path(receipt["receipt"]))
            } == before
            root = Path(spec["state_dir"]) / "technical-successor"
            seal = {"spec": spec, "resources": observed}
            for name, raw in before.items():
                original = root / "predecessor-resources" / name
                original.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
                original.write_bytes(raw)
                original.chmod(0o600)
            write_private(root / "closure-intent.json", {"predecessor_resources": observed})
            fresh = technical._closure_cleanup(seal, root)
            assert fresh["state"] == "confirmed" and not scratch.exists()
            assert (
                read_private(root / "predecessor-resources/finalization.json")["state"] == "unknown"
            )
            assert all(
                (root / "predecessor-resources" / name).read_bytes() == raw
                for name, raw in before.items()
            )
    finally:
        if lock:
            lock.close()




def test_complete_prospective_tree_and_signed_one_merge_replay_use_only_owned_fixture_git(tmp_path):
    repo, remote = tmp_path / "source", tmp_path / "remote.git"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.name", "Integration Fixture")
    _git(repo, "config", "user.email", "fixture@example.invalid")
    for i in range(6):
        (repo / f"overlap-{i}.txt").write_text(
            "".join(f"context {n}\n" for n in range(10))
            + ("138-member\n" if i == 0 else "left base\n")
            + "".join(f"middle {n}\n" for n in range(10))
            + "right base\n"
        )
    worker = repo / "workers/automation/pyproject.toml"
    worker.parent.mkdir(parents=True)
    worker.write_text(
        '[project]\nname="fixture"\ndependencies=[]\n'
        '[tool.hatch.build]\nartifacts=["asset.txt", "migrations/*.sql"]\n'
        '[tool.pytest.ini_options]\naddopts="-q"\n'
    )
    (repo / "package.json").write_text('{"name":"fixture"}\n')
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "test: original source")
    base = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-qb", "fix/owned")
    for i in range(6):
        path = repo / f"overlap-{i}.txt"
        path.write_text(
            path.read_text().replace(
                "138-member\n" if i == 0 else "left base\n",
                "COACHING ASSERTION PRESERVED\n138-member\n" if i == 0 else "left owned\n",
            )
        )
    (repo / "owned-only.txt").write_text("preserved owned source\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "feat: original coaching source")
    owned = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "main")
    for i in range(6):
        path = repo / f"overlap-{i}.txt"
        path.write_text(
            path.read_text().replace(
                "138-member\n" if i == 0 else "right base\n",
                "143-member\n" if i == 0 else "right main\n",
            )
        )
    worker.write_text(
        worker.read_text().replace(
            '"migrations/*.sql"', '"src/jobctrl/assets/interview/*.json", "migrations/*.sql"'
        )
        + 'tmp_path_retention_policy="failed"\n'
    )
    (repo / "main-only.txt").write_text("retained current main\n")
    _git(repo, "add", ".")
    _git(repo, "commit", "-qm", "feat: current main inputs")
    main = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", "fix/owned")
    _git(tmp_path, "init", "--bare", "-q", str(remote))
    _git(repo, "remote", "add", "origin", str(remote))
    _git(repo, "push", "-q", "origin", "main", "fix/owned")
    scopes = {
        "frozen_original_base": base,
        "owned_predecessor_head": owned,
        "authorized_current_main": main,
        "conflict_path": "overlap-0.txt",
    }
    base_index, owned_index, main_index = [
        integration._index(repo, sha) for sha in (base, owned, main)
    ]
    classified, expected = [], dict(main_index)
    expected["owned-only.txt"] = owned_index["owned-only.txt"]
    for i in range(6):
        path = f"overlap-{i}.txt"
        raw = integration._merged(
            *(
                integration._blob(repo, index[path]["oid"])
                for index in (base_index, owned_index, main_index)
            ),
            conflict=i == 0,
        )
        oid = integration._object("blob", raw)
        expected[path] = {"mode": "100644", "oid": oid}
        classified.append(
            {
                "path": path,
                "bytes": len(raw),
                "expected_blob_sha1": oid,
                "content_sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
    packet = {
        "authority_sha256": "a" * 64,
        "original_base": base,
        "owned_head": owned,
        "authorized_main": main,
        "classified_merged_paths": classified,
        "expected_file_count": len(expected),
        "index": expected,
        "expected_complete_tree_sha1": integration.tree_id(expected),
    }
    spec = {
        "checkout": str(repo),
        "base_sha": base,
        "source_path": str(repo),
        "run_id": "run",
        "branch": "fix/owned",
        "origin_url": str(remote),
    }
    objects_before = set((repo / ".git/objects").rglob("*"))
    bad = deepcopy(packet)
    bad["index"]["main-only.txt"] = owned_index["owned-only.txt"]
    with pytest.raises(ValueError, match="complete prospective"):
        integration.prospective(spec, scopes, bad, "a" * 64)
    observed = integration.prospective(spec, scopes, packet, "a" * 64)
    assert set((repo / ".git/objects").rglob("*")) == objects_before
    assert _git(repo, "rev-parse", "HEAD") == owned
    delta = [
        item
        for item in observed["preparation_inputs"]
        if item["before_sha256"] != item["after_sha256"]
    ]
    assert [item["path"] for item in delta] == ["workers/automation/pyproject.toml"]
    key = tmp_path / "signing-key"
    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)],
        check=True,
        capture_output=True,
    )
    allowed = tmp_path / "allowed-signers"
    allowed.write_text("fixture@example.invalid " + key.with_suffix(".pub").read_text())
    _git(repo, "config", "gpg.format", "ssh")
    _git(repo, "config", "user.signingkey", str(key))
    _git(repo, "config", "gpg.ssh.allowedSignersFile", str(allowed))
    broker = SimpleNamespace(checkout=repo, source=repo, spec=spec)
    broker._existing_pr = lambda **_kw: {
        "number": 7,
        "state": "OPEN",
        "isDraft": False,
        "headRefOid": _git(repo, "ls-remote", "origin", "refs/heads/fix/owned").split()[0],
    }
    grant = {
        "integration": {
            "tree": observed["tree"],
            "old_head": owned,
            "main": main,
            "subject": "chore: integrate preserved source",
            "signer": "Integration Fixture <fixture@example.invalid>",
        },
        "state": {"pull_request": {"number": 7}},
    }
    root = tmp_path / "receipt"
    root.mkdir(mode=0o700)
    head = integration.integrate(broker, grant, observed, root)
    assert _git(repo, "show", "-s", "--format=%P", head).split() == [owned, main]
    assert _git(repo, "show", "-s", "--format=%G?", head) == "G"
    assert _git(repo, "rev-parse", "HEAD^{tree}") == packet["expected_complete_tree_sha1"]
    assert "COACHING ASSERTION PRESERVED\n143-member" in (repo / "overlap-0.txt").read_text()
    receipt = (root / "integration.json").read_bytes()
    assert integration.integrate(broker, grant, observed, root) == head
    assert (root / "integration.json").read_bytes() == receipt
    assert _git(repo, "rev-list", "--count", main + "..HEAD") == "2"


@pytest.mark.parametrize(
    "change",
    ["missing", "wrong-hash", "unknown-kind", "grant", "bool", "extra-root", "partial-packet"],
)
def test_public_technical_preflight_refuses_before_intent_claim_archive_or_effect(service, change):
    store, submitted = service
    store.submit(submitted)
    payload = request()
    if change == "missing":
        payload.pop("authority_path")
    elif change == "wrong-hash":
        authority = store.config.state_root / "technical-authority.json"
        write_private(authority, {"decision_owner": "main task"})
        payload["authority_path"] = str(authority)
    elif change == "unknown-kind":
        payload["continuation_kind"] = "ordinary"
    elif change == "grant":
        payload["additional_iterations"] = 1
    elif change == "bool":
        payload["additional_iterations"] = False
    elif change == "extra-root":
        payload["evidence_root"] = str(store.config.state_root / "foreign")
    else:
        payload["prospective_path"] = "/absent/prospective.json"
    with store._connect() as db:
        claim = store.state.claim_for(db, "work-1")
        commands = list(db.execute("SELECT * FROM delivery_commands"))
    for operation in (store.repair_admission_preflight, store.continue_repair):
        with pytest.raises((ValueError, FileNotFoundError)):
            operation("run-1", payload)
    assert not (Path(store.spec("run-1")["state_dir"]) / "technical-successor").exists()
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") == claim
        assert list(db.execute("SELECT * FROM delivery_commands")) == commands
        assert db.execute("SELECT COUNT(*) FROM delivery_technical_successors").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_effects").fetchone()[0] == 0


@pytest.mark.parametrize("stage", ["review", "checks"])
@pytest.mark.parametrize("point", ["preflight", "role"])
def test_inherited_confirmed_blocked_checkpoint_does_not_freeze_fresh_technical_cancel(
    service,
    monkeypatch,
    stage,
    point,
):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, submitted = service
    submit_historical_admission(store, submitted, monkeypatch)
    spec = store.spec("run-1")
    candidate = {"id": "a" * 64, "head": "b" * 40}
    published = {"number": 7, "head": candidate["head"], "candidate": candidate}
    checkpoint = {
        "event": "blocked",
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "status": "blocked",
        "release": True,
        "state": "confirmed",
        "waiting": False,
        "attempts": 1,
        "cycles": 1,
    }
    state = {
        "run_id": "run-1",
        "iteration": 4,
        "revision": 27,
        "candidate_revision": 1,
        "candidate": candidate,
        "pull_request": published,
        "roles": [],
        "findings": [],
        "usage": {},
        "tracker": {},
        "cleanup": "confirmed",
        "outcome": "blocked",
        "phase": "blocked",
        "checks": {"prepublish": {"state": "passed"}, "terminal_tracker_checkpoint": checkpoint},
    }
    recovery = {
        "kind": technical.KIND,
        "execution_spec": spec,
        "maximum_iteration": 4,
        "command": {"additional_iterations": 0},
        "state": state,
        "resume_stage": stage,
        "candidate": candidate,
        "publication": published,
        "session_id": "original",
    }
    frozen = deepcopy(recovery)
    flow = DeliveryWorkflow()
    cancelled = []

    async def wait(predicate, **_kw):
        assert predicate()

    async def project(*_a, **_kw):
        return None

    async def execute(name, body, **_kw):
        if not cancelled and name == (
            "delivery_technical_readback" if point == "preflight" else "delivery_role"
        ):
            assert flow.state["outcome"] is None
            result = await flow.cancel(
                {
                    "expected_revision": flow.state["revision"],
                    "reason": "cancel fresh owning continuation",
                }
            )
            assert result["phase"] == "cancelling"
            cancelled.append(name)
        if name == "delivery_tracker_start":
            return {"state": "consistent"}
        if name == "delivery_role":
            return {
                "role": body["role"],
                "iteration": 4,
                "status": "pass",
                "candidate": candidate,
                "cleanup": "confirmed",
                "session_id": "independent",
                "findings": [],
            }
        return {"state": "passed", "cleanup": "confirmed"}

    monkeypatch.setattr(workflow, "wait_condition", wait)
    monkeypatch.setattr(flow, "_project", project)
    monkeypatch.setattr(flow, "_activity", execute)
    result = asyncio.run(flow.run(spec, recovery))
    assert cancelled and result["outcome"] == "cancelled"
    assert "terminal_tracker_checkpoint" not in result["checks"]
    if stage == "review":
        assert result["checks"]["prepublish"] == frozen["state"]["checks"]["prepublish"]
    else:
        assert result["checks"]["prepublish"] == {"state": "passed", "cleanup": "confirmed"}
    assert recovery == frozen


def test_new_owning_terminal_checkpoint_still_freezes_cancellation(service, monkeypatch):
    store, submitted = service
    store.submit(submitted)
    spec = {**store.spec("run-1"), "terminal_tracker_version": 1}
    flow = DeliveryWorkflow()
    flow.state = {
        "phase": "delivered",
        "execution_state": "terminal",
        "outcome": "delivered",
        "revision": 30,
        "iteration": 4,
        "checks": {},
        "roles": [],
        "cleanup": "confirmed",
    }

    async def wait(predicate, **_kw):
        assert predicate()

    async def execute(*_a, **_kw):
        return {
            "state": "confirmed",
            "process_cleanup": "observed-native-confirmed",
            "resource_cleanup": "confirmed",
        }

    async def pending(_spec, checkpoint):
        assert checkpoint["event"] == "delivered" and checkpoint["status"] == "in-review"
        flow.state.update(phase="waiting_tracker", execution_state="waiting_tracker", outcome=None)
        with pytest.raises(ApplicationError, match="terminal transition is frozen"):
            await flow.cancel({"expected_revision": flow.state["revision"], "reason": "too late"})
        return False

    monkeypatch.setattr(workflow, "now", lambda: datetime(2026, 10, 4, tzinfo=UTC))
    monkeypatch.setattr(workflow, "patched", lambda _name: True)
    monkeypatch.setattr(workflow, "wait_condition", wait)
    monkeypatch.setattr(flow, "_activity", execute)
    monkeypatch.setattr(flow, "_finish_terminal_tracker", pending)
    asyncio.run(flow._project(spec, "delivered", "fresh terminal transition"))
    assert flow.state["checks"]["terminal_tracker_checkpoint"]["event"] == "delivered"
    assert flow.cancel_requested is False


@pytest.mark.parametrize("stage", ["review", "checks"])
@pytest.mark.parametrize("failure", [None, "review", "browser"])
def test_published_technical_checkpoint_never_implements_or_republishes(
    service,
    monkeypatch,
    stage,
    failure,
):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, submitted = service
    submit_historical_admission(store, submitted, monkeypatch)
    spec = store.spec("run-1")
    spec["policy"]["browser_qa"] = {"argv": ["fixture"]}
    candidate = {"id": "a" * 64, "head": "b" * 40}
    publication = {"head": candidate["head"], "candidate": candidate, "number": 7}
    old_checks = {"prepublish": {"state": "passed", "candidate_id": candidate["id"]}}
    state = {
        "run_id": "run-1",
        "iteration": 4,
        "revision": 27,
        "candidate_revision": 1,
        "candidate": candidate,
        "pull_request": publication,
        "roles": [],
        "checks": old_checks,
        "tracker": {},
        "usage": {},
        "findings": [],
        "outcome": "blocked",
        "phase": "blocked",
        "cleanup": "confirmed",
    }
    recovery = {
        "kind": technical.KIND,
        "execution_spec": spec,
        "maximum_iteration": 4,
        "command": {"additional_iterations": 0},
        "state": state,
        "resume_stage": stage,
        "candidate": candidate,
        "publication": publication,
        "session_id": "original",
    }
    flow, calls = DeliveryWorkflow(), []

    async def project(*_a):
        return None

    async def execute(name, body, **_kw):
        calls.append((name, body))
        assert name not in {"delivery_publish", "delivery_prepare"}
        if name == "delivery_role":
            assert body["role"] in {"review", "verify"}
            assert body["iteration"] == 4
            return {
                "role": body["role"],
                "iteration": 4,
                "status": "fail" if failure == body["role"] else "pass",
                "candidate": candidate,
                "cleanup": "confirmed",
                "session_id": "independent",
                "findings": ["fresh finding"] if failure == body["role"] else [],
            }
        if name in {"delivery_tracker_start", "delivery_tracker"}:
            return {"state": "consistent"}
        if name == "delivery_browser_qa":
            return {
                "state": "failed" if failure == "browser" else "passed",
                "cleanup": "confirmed",
                "receipt": "receipt",
                "receipt_sha256": "a" * 64,
                "log": "log",
                "log_sha256": "b" * 64,
                "results": [],
            }
        if name == "delivery_ci":
            assert body["pull_request"] == publication
        return {"state": "passed", "cleanup": "confirmed"}

    monkeypatch.setattr(flow, "_project", project)
    monkeypatch.setattr(flow, "_activity", execute)
    result = asyncio.run(flow.run(spec, recovery))
    assert result["iteration"] == 4
    assert result["outcome"] == ("blocked" if failure else "delivered")
    assert all(body.get("role") != "implement" for _name, body in calls)
    assert sum(name == "delivery_precheck" for name, _body in calls) == (stage == "checks")
    assert result["pull_request"] == publication
    if stage == "review":
        assert result["checks"]["prepublish"] == old_checks["prepublish"]


def test_one_native_child_preserves_consumed_generation_and_explicit_base_lineage(
    payload_update,
    monkeypatch,
):
    store, original, payload, _old, package = payload_update
    predecessor_spec, predecessor_reference = renewal.renew(original, payload, digest(payload))
    predecessor_bytes = Path(predecessor_reference["path"]).read_bytes()
    before_receipt = read_private(Path(predecessor_reference["path"]))
    (package / "__init__.py").write_text(
        (package / "__init__.py").read_text() + "\n# Second owned controlled payload.\n"
    )
    controlled = package.parents[2]
    renewal._git(controlled, "add", ".")
    renewal._git(controlled, "commit", "-qm", "test: technical native child")
    authority = {
        "trigger_bindings": {
            "consumed_native_renewal_authority": {
                "path": payload["preparation_authority_path"],
                "sha256": payload["preparation_authority_sha256"],
            }
        },
        "installed_predecessor": {
            "source": before_receipt["source_revision"],
            "config_sha256": before_receipt["config_sha256"],
        },
    }
    predecessor = {
        "original_spec": original,
        "spec": predecessor_spec,
        "recovery": {
            "execution_spec": predecessor_spec,
            "native_preparation_renewal": predecessor_reference,
        },
    }
    monkeypatch.setattr(technical, "_git", renewal._git)
    next_payload = {
        **request(),
        "expected_source_revision": renewal._git(controlled, "rev-parse", "HEAD"),
        "preparation_authority_path": payload["preparation_authority_path"],
        "preparation_authority_sha256": payload["preparation_authority_sha256"],
    }
    observed = technical._source_readiness(predecessor_spec, next_payload, authority, predecessor)
    proposed = deepcopy(predecessor_spec)
    proposed["base_sha"] = (
        "e" * 40
    )  # Explicit project applicability amendment; native identity is unchanged.
    child, child_reference = renewal.renew(
        proposed,
        next_payload,
        digest(next_payload),
        technical={"readiness": observed, "predecessor": predecessor},
    )
    assert Path(predecessor_reference["path"]).read_bytes() == predecessor_bytes
    assert child_reference["path"] != predecessor_reference["path"]
    assert "/technical-successor/native-generation/" in child_reference["path"]
    assert child["base_sha"] == proposed["base_sha"]
    assert (
        child["preparation"]["security_binding_sha256"]
        != (predecessor_spec["preparation"]["security_binding_sha256"])
    )
    assert renewal.renew(
        proposed,
        next_payload,
        digest(next_payload),
        technical={"readiness": observed, "predecessor": predecessor},
    ) == (child, child_reference)
    recovery = {
        "execution_spec": child,
        "native_preparation_renewal": child_reference,
        "native_predecessor": predecessor,
        "authority": authority,
    }
    assert renewal.effective_spec(proposed, recovery, technical=True) == child
    with pytest.raises(ValueError, match="exact owned path"):
        renewal.effective_spec(proposed, recovery)
    changed = deepcopy(recovery)
    changed["native_predecessor"]["spec"]["base_sha"] = "f" * 40
    with pytest.raises(ValueError, match="predecessor changed"):
        renewal.effective_spec(proposed, changed, technical=True)
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0


def test_complete_tree_hash_and_byte_preserving_single_conflict(tmp_path):
    repo = tmp_path / "fixture"
    repo.mkdir()
    subprocess = __import__("subprocess")
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    (repo / "one").write_bytes(b"ONE")
    (repo / "nested").mkdir()
    (repo / "nested/two").write_bytes(b"TWO")
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    tree = subprocess.check_output(["git", "-C", str(repo), "write-tree"]).decode().strip()
    index = {
        path: {"mode": "100644", "oid": integration._object("blob", raw)}
        for path, raw in [("one", b"ONE"), ("nested/two", b"TWO")]
    }
    assert integration.tree_id(index) == tree
    prefix = b'describe("DemoLocalCommandExecutor", () => {\n'
    title = (
        b'  it("keeps the 138-member capability manifest exhaustive '
        b'with exact class counts", () => {\n'
    )
    coaching = (
        b'  it("accepted coaching", () => {\n    expect(persistence).toEqual(before);\n  });\n\n'
    )
    base = prefix + title + b"    // unchanged context\n" * 10
    base += b"    expect(count).toBe(138);\n  });\n});\n"
    owned = prefix + coaching + base[len(prefix) :]
    main = base.replace(b"138-member", b"143-member").replace(b"toBe(138)", b"toBe(143)")
    result = integration._merged(base, owned, main, conflict=True)
    assert result == prefix + coaching + main[len(prefix) :]
    assert coaching in result
    with pytest.raises(ValueError, match="adjacent inventory title"):
        integration._merged(base, owned, main.replace(b"143-member", b"144-member"), conflict=True)
    assert hashlib.sha256(result).hexdigest() != hashlib.sha256(owned).hexdigest()
