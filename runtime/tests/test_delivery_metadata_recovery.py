from __future__ import annotations

import hashlib
import json
import os

import pytest
from test_delivery_store import _git, submit_historical_admission
from test_delivery_store import service as service

from devflow_temporal import delivery_metadata_recovery as metadata
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker


@pytest.fixture
def published(service, monkeypatch):
    store, request = service
    request["goal"] = "refactor(profile): share pure production and demo coaching policy"
    repository = store.config.raw["repositories"]["fixture"]
    repository["prepublish_checks"] = [
        {
            "id": "diff",
            "argv": ["git", "diff", "--check"],
            "cwd": ".",
            "kind": "check",
            "timeout_seconds": 30,
        }
    ]
    repository["checks"] = repository["prepublish_checks"]
    store.config.path.write_text(json.dumps(store.config.raw))
    submit_historical_admission(store, request, monkeypatch)
    spec = store.spec("run-1")
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    broker.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    roles = []
    for iteration in range(2):
        (broker.checkout / "README.md").write_text(f"Owned content {iteration}\n")
        candidate = broker.candidate()
        role = {
            "role": "implement",
            "iteration": iteration,
            "status": "pass",
            "session_id": "original-session",
            "cleanup": "confirmed",
            "input_candidate_id": candidate["id"],
            "candidate": candidate,
        }
        roles.append(role)
        key = f"publish:run-1:{iteration}"
        broker._effect(
            key, "publish", {"iteration": iteration, "input_candidate_id": candidate["id"]}
        )
        _git(broker.checkout, "add", "README.md")
        _git(broker.checkout, "commit", "-qm", "Implement " + request["goal"])
        after = broker.candidate()
        pr = {
            "number": 7,
            "url": "https://example.invalid/pull/7",
            "state": "OPEN",
            "head": after["head"],
            "base": spec["base_sha"],
            "candidate": after,
        }
        broker._finish_effect(key, pr)
        with store._connect() as db:
            db.execute(
                "INSERT INTO delivery_attempts "
                "(job_key,run_id,role,iteration,candidate_id,state,session_id,"
                "result_json,cleanup) VALUES (?,?,?,?,?,'finished',?,?,'confirmed')",
                (
                    f"implementation-{iteration}",
                    "run-1",
                    "implement",
                    iteration,
                    candidate["id"],
                    "original-session",
                    canonical_json(role),
                ),
            )
    _git(broker.checkout, "push", "origin", "HEAD:refs/heads/feat/fixture")
    state = {
        "run_id": "run-1",
        "revision": 13,
        "iteration": 1,
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "cleanup": "none",
        "error": "repair limit exhausted",
        "candidate": after,
        "pull_request": pr,
        "candidate_revision": 4,
        "roles": roles,
        "checks": {},
        "usage": {},
        "findings": ["Historical browser rejection"],
        "tracker": {},
    }
    store.project(
        "run-1",
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message=state["error"],
        candidate=after,
        pull_request=pr,
        checks={},
        iteration=1,
        protocol_revision=13,
        outcome="blocked",
        cleanup="none",
        error=state["error"],
    )
    closed = {
        "workflow_id": "delivery-run-1",
        "execution_run_id": "authentic-closed-run",
        "closed_at": "2026-10-03T20:00:00Z",
        "request_digest": spec["request_digest"],
        "recovery_digest": None,
        "result": state,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_args, **_kw: closed)
    title = {"value": request["goal"]}

    def existing(self, **_kwargs):
        remote = _git(self.source, "ls-remote", "origin", "refs/heads/feat/fixture").split()[0]
        return {
            "number": 7,
            "url": pr["url"],
            "state": "OPEN",
            "isDraft": False,
            "headRefName": spec["branch"],
            "baseRefName": "HEAD",
            "headRefOid": remote,
            "title": title["value"],
        }

    monkeypatch.setattr(DeliveryBroker, "_existing_pr", existing)
    authority = {
        "decision_owner": "main task",
        "new_user_approval_required": False,
        "authority_source": "Existing authorized original publication repair",
        "scope": {
            "eligible_original_run_ids": ["run-1", "run-2"],
            "repository": "github.com/example/fixture",
            "known_base": spec["base_sha"],
            "max_reconciliation_commands_per_original_run": 1,
            "exact_duplicate_and_known_interrupted_effect_resume_allowed": True,
        },
    }
    path = store.config.state_root / "metadata-authority.json"
    path.write_text(json.dumps(authority))
    path.chmod(0o600)
    command = {
        "command_id": "metadata-1",
        "expected_revision": 13,
        "expected_candidate_id": after["id"],
        "expected_head": after["head"],
        "expected_pr_number": 7,
        "expected_signer": "Delivery Test <delivery@example.invalid>",
        "authority_path": str(path),
        "authority_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
    return store, broker, state, closed, command, title














def restore_admitted_metadata(store, broker, state, closed, command, title):
    """Fixture rows for an already-admitted predecessor; never an admission API or history."""
    from copy import deepcopy

    from devflow_temporal.delivery_resources import private_directory

    spec = broker.spec
    row, attempts, effects, claim = metadata._rows(store, spec['run_id'])
    mapping = []
    parent = spec['base_sha']
    # Construct fixture objects directly. No production reconciliation or remote writer.
    for old in _git(broker.checkout, 'rev-list', '--reverse', parent + '..HEAD').splitlines():
        tree = _git(broker.checkout, 'show', '-s', '--format=%T', old)
        subject = spec['goal'] + '\n\nSigned-off-by: Delivery Test <delivery@example.invalid>'
        new = _git(broker.checkout, 'commit-tree', tree, '-p', parent, '-m', subject)
        mapping.append({'old': old, 'new': new, 'tree': tree,
                        'original_object': _git(broker.checkout, 'cat-file', 'commit', old) + '\n',
                        'new_object': _git(broker.checkout, 'cat-file', 'commit', new) + '\n'})
        parent = new
    grant = {'kind': 'published_metadata_recovery', 'command': command, 'spec': spec,
             'state': deepcopy(state), 'closed': deepcopy(closed), 'original_recovery': None,
             'original_row': row, 'attempts': attempts, 'effects': effects, 'claim': claim,
             'authority': metadata._authority(command, spec), 'mapping': mapping,
             'old_head': state['candidate']['head'], 'new_head': parent}
    root = broker.state_dir / 'metadata-reconciliation'
    private_directory(root)
    metadata._immutable(root / 'intent.json', grant)
    for kind, head in (('original', grant['old_head']), ('rewritten', parent)):
        ref = f"refs/devflow/metadata/{spec['run_id']}/{kind}"
        _git(broker.checkout, 'update-ref', ref, head)
        metadata._immutable(root / (kind + '-ref.json'), {'ref': ref, 'head': head})
    _git(broker.checkout, 'reset', '--hard', parent)
    # Only the fixture's private bare repository; no network or force push.
    origin = store.config.raw['repositories']['fixture']['origin_url']
    _git(origin, 'fetch', str(broker.checkout), parent)
    _git(origin, 'update-ref', 'refs/heads/' + spec['branch'], parent)
    title['value'] = spec['goal']
    candidate = broker.candidate()
    publication = {**state['pull_request'], 'head': parent, 'candidate': candidate}
    recovery = {**grant, 'candidate': candidate, 'publication': publication,
                'grant_digest': digest(grant), 'execution_spec': spec,
                'native_preparation_renewal': None}
    with store._connect() as db:
        db.execute("INSERT INTO delivery_metadata_recoveries VALUES (?,?,?,?,'queued')",
                   (spec['run_id'], command['command_id'], digest(command), canonical_json(grant)))
        db.execute("UPDATE delivery_runs SET recovery_json=?,workflow_id=?,"
                   "phase='metadata_validation_queued',execution_state='queued',outcome=NULL,"
                   "error=NULL WHERE run_id=?",
                   (canonical_json(recovery), 'delivery-' + spec['run_id'] + '-metadata-1',
                    spec['run_id']))
    return recovery


def test_explicit_command_bound_and_immutable_staged_receipt_recovery(published, monkeypatch):
    store, broker, _state, _closed, command, _title = published
    root = broker.state_dir / "metadata-reconciliation"
    root.mkdir(mode=0o700)
    value = {"bounded": "receipt"}
    path = root / "controlled.json"
    original_unlink = metadata.Path.unlink
    stopped = []

    def unlink(self, *args, **kwargs):
        if self.name.startswith(".stage-controlled") and not stopped:
            stopped.append(True)
            raise RuntimeError("interrupted staged link cleanup")
        return original_unlink(self, *args, **kwargs)

    monkeypatch.setattr(metadata.Path, "unlink", unlink)
    with pytest.raises(RuntimeError, match="interrupted"):
        metadata._immutable(path, value)
    assert path.stat().st_nlink == 2
    metadata._immutable(path, value)
    assert path.stat().st_nlink == 1
    os.link(path, root / "foreign-hardlink")
    with pytest.raises(ValueError):
        metadata._immutable(path, value)


def test_metadata_gate_namespace_preserves_retained_old_head_and_artifacts(published):
    from pathlib import Path

    from devflow_temporal.delivery_activities import _context
    from devflow_temporal.delivery_resources import RunResources, private_directory, read_private

    store, broker, state, _closed, command, _title = published
    resources = RunResources(broker.spec)
    old_path = broker.state_dir / 'gates/1/verify'
    private_directory(old_path.parent)
    resources.register(old_path, 'gate')
    old = broker.gate_checkout('verify', 1, state['candidate'])
    resources.created(old)
    old_patch = broker.gate_diff('verify', 1, state['candidate'])
    retained = Path(old_patch['path']).read_bytes()
    restore_admitted_metadata(store, broker, state, _closed, command, _title)
    spec = store.effective_spec('run-1')
    _store, successor = _context(spec)
    candidate = successor.candidate()
    expected = broker.state_dir / 'metadata-reconciliation/evidence/gates/1/verify'
    private_directory(expected.parent)
    resources.register(expected, 'gate')
    path = successor.gate_checkout('verify', 1, candidate)
    resources.created(path)
    assert path == broker.state_dir / 'metadata-reconciliation/evidence/gates/1/verify'
    assert path != old and _git(path, 'rev-parse', 'HEAD') == candidate['head']
    diff = successor.gate_diff('verify', 1, candidate)
    from devflow_temporal.delivery_sandbox import _review_diff_path

    request = {'spec': spec, 'role': 'verify', 'iteration': 1,
               'candidate': candidate, 'workspace': str(path), 'review_diff': diff}
    assert _review_diff_path(request) == Path(diff['path'])
    resources = RunResources(spec)
    resources._allowed(path, 'gate')
    assert str(old) in read_private(resources.manifest)['roots']
    assert _git(old, 'rev-parse', 'HEAD') == state['candidate']['head']
    assert Path(old_patch['path']).read_bytes() == retained
    assert successor.state_dir == broker.state_dir
    generated = path / 'node_modules'
    resources.register(generated, 'generated')
    generated.mkdir()
    resources.created(generated)
    generated.joinpath('owned.txt').write_text('temporary generated dependency')
    resources.finalize('blocked')
    assert not generated.exists()
    assert _git(old, 'rev-parse', 'HEAD') == state['candidate']['head']
    assert Path(old_patch['path']).read_bytes() == retained


@pytest.mark.parametrize('change', ['foreign', 'alias', 'raw-spec', 'head', 'candidate', 'seal'])
def test_metadata_gate_namespace_refuses_foreign_alias_or_changed_custody(published, change):
    from copy import deepcopy

    from devflow_temporal.delivery_activities import _context
    from devflow_temporal.delivery_resources import write_private

    store, original, state, _closed, command, _title = published
    restore_admitted_metadata(store, original, state, _closed, command, _title)
    spec = store.effective_spec('run-1')
    _store, broker = _context(spec)
    candidate = broker.candidate()
    expected = broker.state_dir / 'metadata-reconciliation/evidence/gates/1/verify'
    outside = store.config.state_root / 'foreign-gates'
    outside.mkdir()
    outside.joinpath('sentinel').write_text('unchanged')
    if change == 'foreign':
        broker.evidence_dir = outside
    elif change == 'alias':
        expected.parents[1].symlink_to(outside, target_is_directory=True)
    elif change == 'raw-spec':
        broker.spec = {**spec, 'evidence_root': str(outside)}
    elif change == 'head':
        broker.gate_checkout('verify', 1, candidate)
        _git(expected, 'checkout', '--detach', state['candidate']['head'])
    elif change == 'candidate':
        candidate = {**candidate, 'id': '0' * 64}
    else:
        path = broker.state_dir / 'metadata-reconciliation/intent.json'
        intent = deepcopy(metadata.read_private(path))
        intent['new_head'] = '0' * 40
        write_private(path, intent)
    with pytest.raises((ValueError, RuntimeError)):
        broker.gate_checkout('verify', 1, candidate)
    assert outside.joinpath('sentinel').read_text() == 'unchanged'
    assert sorted(p.name for p in outside.iterdir()) == ['sentinel']
    if change in {'foreign', 'alias', 'raw-spec', 'seal'}:
        assert not expected.exists()
    assert original.state_dir == broker.state_dir
