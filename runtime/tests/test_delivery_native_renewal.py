from __future__ import annotations

import base64
import hashlib
import json
import shutil
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal import delivery_native_preparation as native
from devflow_temporal import delivery_native_renewal as renewal
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_preparation import PreparationError, prepare_authority
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def payload_update(native_configuration, monkeypatch, tmp_path):
    if sys.platform != 'darwin':
        pytest.skip('real macOS native preparation and locked bundled CLI required')
    config, request = native_configuration
    config.raw['execution_mode'] = 'trusted-local'
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(DeliveryConfig.load(config.path))
    store.submit(request)
    # Controlled owned source copy; the installed host/source is never modified.
    source = tmp_path / 'controlled-runtime'
    package = source / 'runtime/src/devflow_temporal'
    shutil.copytree(native.PACKAGE, package, ignore=shutil.ignore_patterns('__pycache__'))
    git = renewal._git
    git(source, 'init', '-q')
    git(source, 'config', 'user.name', 'Native Renewal Test')
    git(source, 'config', 'user.email', 'renewal@example.invalid')
    git(source, 'add', '.')
    git(source, 'commit', '-qm', 'test: retained source')
    monkeypatch.setattr(native, 'PACKAGE', package)
    monkeypatch.setattr(renewal, '_git', lambda _path, *args: git(source, *args))
    spec = prepare_authority(store, store.spec(request['run_id']))
    spec = deepcopy(spec)
    spec['role_home_generation'] = 'policy-1'
    spec['terminal_tracker_version'] = 1
    spec = native.bind_native_spec(spec, spec['policy']['native_identity'],
                                  Path(spec['preparation']['environment']['path']), reused=True)
    old = Path(spec['preparation']['environment']['path']).read_bytes()
    (package / '__init__.py').write_text((package / '__init__.py').read_text()
                                      + '\n# Controlled payload-only revision.\n')
    git(source, 'add', '.')
    git(source, 'commit', '-qm', 'test: new payload')
    trigger = config.state_root / 'delta.json'
    write_private(trigger, {'before': spec['policy']['native_identity'],
                            'after': native.native_identity(spec)})
    authority = config.state_root / 'renewal-authority.json'
    write_private(authority, {'decision_owner': 'main task',
                             'authority_source': 'Controlled fixture',
                             'new_user_approval_required': False, 'runs': [request['run_id']],
                             'max_generations_per_run': 1, 'max_total_generations': 2,
                             'provider_turns_for_renewal': 0, 'implementation_turns_for_renewal': 0,
                             'new_repair_grants_for_renewal': 0,
                             'trigger_path': str(trigger),
                             'trigger_sha256': hashlib.sha256(trigger.read_bytes()).hexdigest()})
    payload = {'command_id': 'same-stopped-command', 'preparation_authority_path': str(authority),
               'preparation_authority_sha256': hashlib.sha256(authority.read_bytes()).hexdigest()}
    return store, spec, payload, old, package


@pytest.mark.parametrize('home_generation', ['policy-1', None])
def test_payload_only_native_generation_preserves_old_proof_and_replays_strictly(
    payload_update, home_generation,
):
    store, before, payload, old, _package = payload_update
    if home_generation is None:
        before.pop('role_home_generation')
    historical = deepcopy(before)
    native.verify_native_spec(before)
    assert before == historical
    assert Path(before['preparation']['environment']['path']).read_bytes() == old
    assert json.loads(old)['identity'] == historical['policy']['native_identity']
    current = native.native_identity(before)
    frozen = historical['policy']['native_identity']
    assert current['runtime_payload_sha256'] != frozen['runtime_payload_sha256']
    assert {key: value for key, value in current.items() if key != 'runtime_payload_sha256'} == {
        key: value for key, value in frozen.items() if key != 'runtime_payload_sha256'
    }
    for mismatch in ('sandbox', 'fingerprint'):
        changed = deepcopy(before)
        if mismatch == 'sandbox':
            changed['policy']['host_sandbox'] = 'native-profile'
            changed['policy_digest'] = digest(changed['policy'])
        else:
            changed['preparation']['fingerprint'] = '0' * 64
        with pytest.raises(PreparationError):
            native.verify_native_spec(changed)
    after, reference = renewal.renew(before, payload, digest(payload))
    assert after.get('role_home_generation') == home_generation
    assert Path(before['preparation']['environment']['path']).read_bytes() == old
    assert after['preparation']['fingerprint'] != before['preparation']['fingerprint']
    assert after['policy']['native_identity']['protected_commands'] == (
        before['policy']['native_identity']['protected_commands']
    )
    native.verify_native_spec(after)
    assert renewal.renew(before, payload, digest(payload)) == (after, reference)
    assert renewal.verify_generation(before, reference, after) == after
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 0
        assert db.execute('SELECT COUNT(*) FROM delivery_repair_grants').fetchone()[0] == 0
    generation = read_private(Path(reference['path']))
    assert generation['original_spec'] == before
    assert len(generation['measurement_copies']) == 3
    for retained in generation['measurement_copies']:
        copied = read_private(Path(retained['path']))
        raw = base64.b64decode(copied['content'], validate=True)
        assert hashlib.sha256(raw).hexdigest() == retained['original_sha256']
        assert raw == Path(retained['original_path']).read_bytes()
    index = {item['id'] for item in store.evidence_index(before['run_id'])}
    assert 'native-preparation-renewal-generation' in index
    assert 'native-preparation-renewal-proof' in index
    assert 'native-preparation-renewal-measurement-log' in index
    assert any(item.startswith('native-renewal-probe-0-') for item in index)
    assert all(a['cleanup']['state'] == 'confirmed' for a in generation['preparation_history'])
    with pytest.raises(ValueError):
        renewal.renew(before, {**payload, 'command_id': 'different-command'}, 'f' * 64)


@pytest.mark.parametrize('change', ['protected-root', 'authority'])
def test_native_generation_rejects_every_non_payload_or_unapproved_change(
    payload_update, monkeypatch, change,
):
    _store, spec, payload, _old, _package = payload_update
    if change in {'protected-root', 'binary'}:
        identity = native.native_identity(spec)
        identity['protected_commands' if change == 'protected-root' else 'codex_bin_sha256'] = (
            ['/foreign'] if change == 'protected-root' else '0' * 64
        )
        monkeypatch.setattr(renewal, 'native_identity', lambda _spec: identity)
    elif change == 'mode':
        spec['policy']['host_sandbox'] = 'native-profile'
    elif change == 'config':
        # The sealed snapshot is the authority: only its loss or tampering rejects.
        snapshot = (Path(spec['state_dir']).parents[1] / 'configuration-snapshots'
                    / (spec['config_digest'] + '.json'))
        snapshot.unlink()
        Path(spec['config_path']).write_text('{}')
    else:
        payload['preparation_authority_sha256'] = '0' * 64
    with pytest.raises((ValueError, OSError)):
        renewal.renew(spec, payload, digest(payload))
    assert not (Path(spec['state_dir']) / 'native-preparation-renewal/generation.json').exists()


def test_native_generation_resumes_interrupted_probe_and_retains_failure(
    payload_update, monkeypatch,
):
    _store, spec, payload, old, _package = payload_update
    original = native._measure
    calls = []

    def interrupted(*args, **kwargs):
        observed = original(*args, **kwargs)
        calls.append(observed)
        if len(calls) == 1:
            raise RuntimeError('Controlled uncertain measurement')
        return observed

    monkeypatch.setattr(native, '_measure', interrupted)
    with pytest.raises(RuntimeError, match='uncertain measurement'):
        renewal.renew(spec, payload, digest(payload))
    assert Path(spec['preparation']['environment']['path']).read_bytes() == old
    after, reference = renewal.renew(spec, payload, digest(payload))
    native.verify_native_spec(after)
    history = read_private(Path(reference['path']))['preparation_history']
    assert len(history) == 2 and 'Controlled uncertain' in history[0]['error']
    assert all(attempt['cleanup']['state'] == 'confirmed' for attempt in history)
    assert renewal.renew(spec, payload, digest(payload)) == (after, reference)
    assert len(calls) == 2




@pytest.mark.parametrize('bad', ['missing', 'wrong-hash'])
def test_readonly_native_readiness_rejects_authority_without_sealing(payload_update, bad):
    _store, spec, payload, old, _package = payload_update
    supplied = ({'command_id': payload['command_id']} if bad == 'missing' else
                {**payload, 'preparation_authority_sha256': '0' * 64})
    with pytest.raises(ValueError):
        renewal.readiness(spec, supplied)
    assert not (Path(spec['state_dir']) / 'native-preparation-renewal').exists()
    assert Path(spec['preparation']['environment']['path']).read_bytes() == old
    assert renewal.readiness(spec, payload)['required'] is True
    assert not (Path(spec['state_dir']) / 'native-preparation-renewal').exists()


def test_native_generation_recovers_a_recorded_started_attempt(payload_update, monkeypatch):
    _store, spec, payload, _old, _package = payload_update
    original = native._measure
    calls = []

    def interrupted(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(result)
        if len(calls) == 1:
            raise RuntimeError('Controlled interrupted owner')
        return result

    monkeypatch.setattr(native, '_measure', interrupted)
    with pytest.raises(RuntimeError, match='interrupted owner'):
        renewal.renew(spec, payload, digest(payload))
    path = Path(spec['state_dir']) / 'native-preparation-renewal/preparation.json'
    journal = read_private(path)
    # Recreate the lost acknowledgement window while preserving real probe evidence.
    journal['preparation_attempts'][0]['state'] = 'started'
    journal['preparation_attempts'][0]['cleanup'] = None
    write_private(path, journal)
    after, reference = renewal.renew(spec, payload, digest(payload))
    native.verify_native_spec(after)
    history = read_private(Path(reference['path']))['preparation_history']
    assert [item['state'] for item in history] == ['interrupted', 'stopped']
    assert all(item['cleanup']['state'] == 'confirmed' for item in history)
    assert len(calls) == 2
