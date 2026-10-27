"""Controller tails retain raw failed history; fixture seams never claim native execution."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from test_delivery_store import service as service
from test_delivery_store import submit_historical_admission

from devflow_temporal import delivery_investigation_adjudication as adjudication
from devflow_temporal import delivery_resource_closure as closure
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import RunResources


def request(kind):
    return {
        "continuation_kind": kind,
        "command_id": "final-1",
        "expected_revision": 41,
        "expected_iteration": 4,
        "expected_candidate_id": "a" * 64,
        "expected_pr_number": 7,
        "expected_pr_head": "b" * 40,
        "additional_iterations": 0,
        "authority_path": "/private/authority.json",
        "authority_sha256": "c" * 64,
        "controller_path": "/private/controller.json",
        "controller_sha256": "d" * 64,
    }


@pytest.mark.parametrize("kind", [adjudication.KIND, closure.KIND])
@pytest.mark.parametrize("bad", ["missing", "child"])
def test_public_discriminator_refuses_before_ordinary_grant_or_any_effect(
    service, monkeypatch, kind, bad
):
    store, submitted = service
    store.submit(submitted)
    payload = request(kind)
    if bad == "missing":
        payload.pop("controller_path")
    elif bad == "grant":
        payload["additional_iterations"] = 1
    elif bad == "boolean":
        payload["additional_iterations"] = False
    elif bad == "iteration":
        payload["expected_iteration"] = 5
    elif bad == "root":
        payload["evidence_root"] = "/foreign"
    else:
        monkeypatch.setenv("DEVFLOW_MANAGED_DEPTH", "1")
    with store._connect() as db:
        claim = store.state.claim_for(db, "work-1")
        commands = list(db.execute("SELECT * FROM delivery_commands"))
    for operation in (store.repair_admission_preflight, store.continue_repair):
        with pytest.raises(ValueError):
            operation("run-1", payload)
    with store._connect() as db:
        assert store.state.claim_for(db, "work-1") == claim
        assert list(db.execute("SELECT * FROM delivery_commands")) == commands
        assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_resource_closures").fetchone()[0] == 0
    root = Path(store.spec("run-1")["state_dir"])
    assert not (root / "resource-closure").exists()
    assert not (root / "investigation-adjudication").exists()


@pytest.fixture
def stopped_tail(service, monkeypatch):
    store, submitted = service
    submit_historical_admission(store, submitted, monkeypatch)
    spec = store.spec("run-1")
    spec["resource_cleanup_version"] = 1
    spec["terminal_tracker_version"] = 1
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    candidate = broker.candidate()
    publication = {
        "number": 7,
        "head": candidate["head"],
        "candidate": candidate,
        "url": "https://example.invalid/pull/7",
        "state": "OPEN",
    }
    final = RunResources(spec).finalize("blocked", uncertain=True)
    state = {
        "run_id": "run-1",
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "error": "repair limit exhausted",
        "cleanup": "unknown",
        "iteration": 4,
        "revision": 41,
        "candidate_revision": 9,
        "candidate": candidate,
        "pull_request": publication,
        "roles": [
            {
                "role": "verify",
                "iteration": 4,
                "status": "findings",
                "cleanup": "confirmed",
                "findings": ["Medium receipt", "Medium layout", "Medium orphans"],
            }
        ],
        "checks": {
            "resource_cleanup": final,
            "terminal_tracker_checkpoint": {"state": "confirmed"},
            "qa": {"state": "failed"},
            "browser_qa": {"state": "failed", "test_count": 5},
        },
        "findings": ["Medium receipt", "Medium layout", "Medium orphans"],
        "tracker": {},
        "usage": {},
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
        protocol_revision=41,
        outcome="blocked",
        cleanup="unknown",
        error=state["error"],
    )
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
        claim = store.state.claim_for(db, "work-1")
    resource_root = Path(spec["state_dir"]) / "resources"
    resources = {
        name + "_sha256": hashlib.sha256((resource_root / file).read_bytes()).hexdigest()
        for name, file in [("manifest", "manifest.json"), ("finalization", "finalization.json")]
    }
    resources.update(journal_sha256={}, roots={})
    return store, spec, state, row, claim, resources


def seal_for(stopped_tail, kind):
    store, spec, state, row, claim, resources = stopped_tail
    payload = request(kind)
    payload.update(
        expected_candidate_id=state["candidate"]["id"], expected_pr_head=state["candidate"]["head"]
    )
    seal = {
        "kind": kind,
        "command": payload,
        "spec": spec,
        "execution_spec": spec,
        "authority": {"raw_qa": {"findings": state["findings"]}},
        "controller": {},
        "original_row": row,
        "original_recovery": None,
        "state": state,
        "resources": resources,
        "claim": claim,
        "parent": {},
        "candidate": state["candidate"],
        "publication": state["pull_request"],
        "maximum_iteration": 4,
    }
    return store, payload, seal


def test_consumed_adjudication_command_remains_read_only(stopped_tail, monkeypatch):
    from devflow_temporal.contracts import canonical_json, digest

    store, payload, seal = seal_for(stopped_tail, adjudication.KIND)
    response = {'run_id': 'run-1', 'workflow_id': 'historical-adjudication',
                'additional_iterations': 0, 'existing': False}
    command_digest = digest({'run_id': 'run-1', **payload})
    with store._connect() as db:
        db.execute('INSERT INTO delivery_commands VALUES (?,?,?,?)',
                   (payload['command_id'], 'run-1', command_digest, canonical_json(response)))
        db.execute('UPDATE delivery_runs SET recovery_json=? WHERE run_id=?',
                   (canonical_json(seal), 'run-1'))
        before = list(db.execute('SELECT * FROM delivery_commands'))
        run = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?', ('run-1',)).fetchone())
        claim = store.state.claim_for(db, 'work-1')
    observed = []
    monkeypatch.setattr(adjudication, 'readback', lambda *args: observed.append(args))
    for operation in (store.repair_admission_preflight, store.continue_repair):
        assert operation('run-1', payload)['existing'] is True
    assert len(observed) == 2
    with store._connect() as db:
        assert list(db.execute('SELECT * FROM delivery_commands')) == before
        assert dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                               ('run-1',)).fetchone()) == run
        assert store.state.claim_for(db, 'work-1') == claim
