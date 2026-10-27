"""CI-only custody follows a real admitted interrupted-provider predecessor.

The provider, remote GitHub response and original Temporal closure are controlled
boundaries. SQLite commands, stopped-resume admission, frozen configuration,
resource journals, current check processes and public HTTP commands remain real.
"""

from __future__ import annotations

import asyncio
import copy
import json
import os
from importlib.metadata import distribution
from pathlib import Path
from subprocess import CompletedProcess
from types import SimpleNamespace

import pytest
from ci_inventory_fixture import NAMES, approve, fixture
from test_delivery_blocked_scope import prepared_spec
from test_delivery_browser_mode_recovery import restore_native_observers
from test_delivery_ci_inventory import public
from test_delivery_quarantined_authority import admitted, native_case
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_ci_inventory as inventory
from devflow_temporal import delivery_ci_teardown as teardown
from devflow_temporal import delivery_gate_retry as gates
from devflow_temporal import delivery_policy_recovery as policy
from devflow_temporal import delivery_resources as resources
from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_activities import delivery_gates_readback, delivery_repair_preflight
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_ci_inventory_io import GitHubRead
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def historical_ci(service, monkeypatch):
    initial, request = service
    # Keep the real canonical native ownership rule inside an owned user home;
    # this fixture must never read or mutate the operator's execution registry.
    home = initial.config.path.parent / "owned-home"
    home.mkdir()
    monkeypatch.setattr(Path, "home", classmethod(lambda _cls: home))
    config = copy.deepcopy(initial.config.raw)
    config.update(
        provider="codex",
        execution_mode="trusted-local",
        max_repairs=10,
        execution_registry=str(home / ".local/state/devflow/execution-ownership/registry.sqlite3"),
        codex_bin=str(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex")),
    )
    config["repositories"]["fixture"].update(
        required_ci=NAMES[:],
        checks=[{"id": "local", "argv": ["/usr/bin/true"]}],
        prepublish_checks=[{"id": "prepublish", "argv": ["/usr/bin/true"]}],
        project_url="https://github.com/users/example/projects/1",
        assignee="example",
    )
    initial.config.path.write_text(canonical_json(config))
    store = DeliveryStore(DeliveryConfig.load(initial.config.path))
    historical = native_case((store, request), monkeypatch, preserve_existing_roots=True)
    # The oldest legacy fixture replaces this consumer. Restore its actual
    # owner so the pre-ACK refusal and the purpose-specific bridge are tested.
    monkeypatch.setattr(gates, "_stopped_cleanup", policy._stopped_cleanup)
    monkeypatch.setattr(resume, "prepare_runtime", lambda value, *_args: prepared_spec(value))
    try:
        response, current, previous = admitted(historical)
    except ValueError as exc:
        if str(exc) != 'predecessor has a live root user or unreadable lease':
            raise
        facts = {'root_kind': None, 'inspection_returncode': None, 'holder_pids': None,
                 'holder_count_capped': None, 'self_pid_reported': None, 'stderr_present': None}
        trace = exc.__traceback__
        while trace is not None:
            if trace.tb_frame.f_code is resources._observe_locked_resources.__code__:
                observed = trace.tb_frame.f_locals.get('observed')
                entry = trace.tb_frame.f_locals.get('entry')
                kind = entry.get('kind') if isinstance(entry, dict) else None
                if isinstance(kind, str):
                    facts['root_kind'] = kind if kind in {'checkout', 'gate'} else 'other'
                if isinstance(observed, CompletedProcess):
                    code = observed.returncode
                    if type(code) is int and -255 <= code <= 255:
                        facts['inspection_returncode'] = code
                    if isinstance(observed.stderr, str):
                        facts['stderr_present'] = bool(observed.stderr)
                    text = observed.stdout
                    if isinstance(text, str) and len(text) <= 4096:
                        lines = text.splitlines()
                        if all(line.startswith('p') and line[1:].isascii()
                               and line[1:].isdigit() and len(line[1:]) <= 10
                               and 0 < int(line[1:]) <= 2**31 - 1 for line in lines):
                            pids = [int(line[1:]) for line in lines]
                            facts.update(holder_pids=pids[:8],
                                         holder_count_capped=min(8, len(pids)),
                                         self_pid_reported=os.getpid() in pids)
                break
            trace = trace.tb_next
        exc.add_note('historical root-user observation: ' + json.dumps(facts, sort_keys=True))
        raise
    assert asyncio.run(delivery_repair_preflight({"spec": current, "recovery": previous})) == {
        "state": "confirmed"
    }
    store.mark_start("run-1", accepted=True)
    broker = DeliveryBroker(store, current)
    _git(broker.checkout, "add", "README.md")
    _git(broker.checkout, "commit", "-qm", "Publish recovered owned subject")
    _git(broker.checkout, "push", "origin", "HEAD:refs/heads/" + current["branch"])
    candidate = broker.candidate()
    publication = {
        "number": 12,
        "head": candidate["head"],
        "state": "OPEN",
        "base": current["base_sha"],
        "url": "https://github.com/example/fixture/pull/12",
        "candidate": candidate,
    }
    monkeypatch.setattr(
        DeliveryBroker,
        "_existing_pr",
        lambda *_args, **_kw: {
            "number": 12,
            "headRefOid": candidate["head"],
            "state": "OPEN",
            "isDraft": False,
            "url": publication["url"],
        },
    )
    restore_native_observers(monkeypatch)
    local = broker.run_checks(1, candidate)
    prepublish = broker.run_prechecks(1, candidate)
    assert local["state"] == prepublish["state"] == "passed"
    final = RunResources(current).finalize("blocked")
    assert final["state"] == "confirmed", final
    error = "required CI did not confirm this PR head"
    checks = {
        "local": local,
        "prepublish": prepublish,
        "review": {"state": "passed", "candidate_id": candidate["id"]},
        "qa": {"state": "passed", "candidate_id": candidate["id"]},
        "ci": {"state": "pending", "reason": "CI deadline reached", "head": candidate["head"]},
        "resource_cleanup": final,
    }
    later = copy.deepcopy(previous["state"])
    later.update(
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        revision=40,
        iteration=1,
        candidate=candidate,
        pull_request=publication,
        cleanup="confirmed",
        error=error,
        checks=checks,
    )
    later["roles"].extend(
        {
            "role": role,
            "iteration": 1,
            "status": "pass",
            "findings": [],
            "session_id": "owned-current-" + role,
            "candidate": candidate,
            "cleanup": "confirmed",
            "finish_reason": "done",
        }
        for role in ("implement", "review", "verify")
    )
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        outcome="blocked",
        event_type="blocked",
        message=error,
        error=error,
        checks=checks,
        candidate=candidate,
        pull_request=publication,
        iteration=1,
        protocol_revision=40,
        cleanup="confirmed",
    )
    with store._connect() as db:
        db.execute("UPDATE delivery_outbox SET state='sent'")
        store.state.release_work(db, current["work_id"], "external:devflow:run-1")
        row = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    closed = {
        "workflow_id": response["workflow_id"],
        "execution_run_id": "closed-recovered-CI",
        "closed_at": "2026-10-26T00:00:00+00:00",
        "request_digest": current["request_digest"],
        "recovery_digest": digest(previous),
        "result": later,
    }
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result", lambda *_args, **_kw: closed)
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_args, **_kw: closed)
    remote = fixture(
        head=candidate["head"],
        tree=_git(broker.checkout, "rev-parse", "HEAD^{tree}"),
        branch=current["branch"],
        policy_digest=current["policy_digest"],
        candidate_id=candidate["id"],
        content_sha256=candidate["content_sha256"],
        pr_number=12,
    )
    remote.profile["subject"]["intake_policy_digest"] = store.intake_execution_spec("run-1")[
        "policy_digest"
    ]
    profile_sha = approve(monkeypatch, remote)
    path = store.config.state_root / "owned-historical-ci-profile.json"
    path.write_text(canonical_json(remote.profile))
    path.chmod(0o600)
    monkeypatch.setattr(
        inventory, "GitHubRead", lambda **kw: GitHubRead(**kw, runner=remote.transport)
    )
    command = {
        "command_id": "historical-ci-inventory",
        "expected_revision": 40,
        "expected_projection_revision": row["revision"],
        "expected_iteration": 1,
        "expected_candidate_id": candidate["id"],
        "expected_candidate_head": candidate["head"],
        "expected_content_sha256": candidate["content_sha256"],
        "expected_pr_number": 12,
        "profile_path": str(path),
        "expected_profile_sha256": profile_sha,
    }
    gate = {
        "continuation_kind": "published_ci_retry",
        "command_id": "historical-ci-observation",
        "expected_revision": 40,
        "expected_iteration": 1,
        "expected_candidate_id": candidate["id"],
        "expected_pr_number": 12,
        "expected_pr_head": candidate["head"],
        "additional_iterations": 0,
    }
    return SimpleNamespace(
        store=store,
        historical=historical,
        current=current,
        previous=previous,
        closed=closed,
        remote=remote,
        command=command,
        gate=gate,
        candidate=candidate,
    )


@pytest.mark.asyncio
async def test_public_ci_ack_and_admission_preserve_original_unknown(historical_ci):
    c = historical_ci
    original = c.historical.journal.read_bytes()
    original_result = copy.deepcopy(c.previous["state"])
    async with public(c) as (client, headers):
        prefix = "/api/runs/run-1/"
        generic = await client.post(prefix + "repair-admission-preflight", json=c.gate)
        assert generic.status_code == 409, generic.text
        preflight = await client.post(prefix + "ci-inventory-preflight", json=c.command)
        assert preflight.status_code == 200, preflight.text
        acknowledgement = {
            **c.command,
            "preflight_key": preflight.json()["preflight_key"],
            "acknowledgement": inventory.ACK,
        }
        ack = await client.post(prefix + "ack-ci-inventory", json=acknowledgement, headers=headers)
        assert ack.status_code == 200, ack.text
        assert ack.json()["delivery_acceptance"] is False
        replay = await client.post(
            prefix + "ack-ci-inventory", json=acknowledgement, headers=headers
        )
        assert replay.status_code == 200 and replay.json()["replayed"] is True, replay.text
        gate_preflight = await client.post(prefix + "repair-admission-preflight", json=c.gate)
        assert gate_preflight.status_code == 200, gate_preflight.text
        admitted_response = await client.post(
            prefix + "continue-repair", json=c.gate, headers=headers
        )
        assert admitted_response.status_code == 200, admitted_response.text
    current = c.store.effective_spec("run-1")
    with c.store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0])
    assert current["gate_retry_stage"] == "ci"
    assert recovery["command"]["additional_iterations"] == 0
    assert teardown.FIELD in recovery["seal"]
    assert inventory.FIELD in recovery["seal"]
    assert await delivery_gates_readback({"spec": current, "recovery": recovery}) == recovery[
        "publication"
    ]
    assert teardown.for_resources(current)["journal_sha256"]
    registry = RunResources(current)
    with registry.locked() as manifest:
        with registry._closed_journals(manifest, manifest["processes"]) as observed:
            assert len(observed) == len(manifest["processes"])
    assert c.historical.journal.read_bytes() == original
    assert c.previous["state"] == original_result
    assert read_private(c.historical.journal)["result"]["provider_outcome"] == "unknown"
    assert read_private(c.historical.journal)["monitoring_complete"] is False


@pytest.mark.parametrize("defect", ["journal", "command", "unadmitted_actor"])
def test_ci_historical_custody_refuses_changed_or_unadmitted_actors(historical_ci, defect):
    c = historical_ci
    if defect == "journal":
        value = read_private(c.historical.journal)
        value["result"]["provider_outcome"] = "passed"
        write_private(c.historical.journal, value)
    elif defect == "command":
        with c.store._connect() as db:
            db.execute(
                "UPDATE delivery_commands SET response_json='{}' WHERE command_id=?",
                (c.previous["command"]["command_id"],),
            )
    else:
        path = Path(c.current["state_dir"]) / "attempts/unadmitted/native-process.json"
        write_private(path, {"phase": "authorized", "monitoring_complete": False})
        RunResources(c.current).process(path)
    with pytest.raises((ValueError, OSError, KeyError)):
        inventory.preflight(c.store, "run-1", c.command)
    with c.store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_ci_inventories").fetchone()
        assert not db.execute("SELECT 1 FROM delivery_ci_inventory_results").fetchone()
