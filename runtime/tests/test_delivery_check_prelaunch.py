from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_resources import spec
from test_delivery_store import _git

from devflow_temporal.delivery_check_prelaunch import observe
from devflow_temporal.delivery_plan_checks import planned_checks
from devflow_temporal.delivery_resources import RunResources, private_directory, write_private


@pytest.fixture
def stopped_check(tmp_path, monkeypatch):
    owned = spec(tmp_path)
    owned['policy'].update(host_sandbox='trusted-local', checks=[])
    owned['accepted_plan'] = json.dumps({'verification': ['Run test_owned.py']})
    root = Path(owned['state_dir'])
    gate = root / 'gates/0/verify'
    private_directory(gate.parent)
    resources = RunResources(owned)
    resources.register(gate, 'gate')
    gate.mkdir(mode=0o700)
    resources.created(gate)
    project = gate / 'worker'
    (project / 'tests').mkdir(parents=True)
    (project / 'tests/test_owned.py').write_text('def test_owned(): assert True\n')
    (project / 'pyproject.toml').write_text('[project]\nname="fixture"\nversion="1"\n')
    (project / 'uv.lock').write_text('version=1\n')
    _git(gate, 'init', '-q')
    _git(gate, 'add', '.')
    manager = tmp_path / 'uv'
    manager.write_bytes(b'fixed-manager')
    monkeypatch.setattr('devflow_temporal.delivery_plan_checks.shutil.which',
                        lambda _: str(manager))
    deps = planned_checks(owned, gate, root / 'checks/0')[0]
    home = resources.scratch('checks', 'checks/0/' + deps['id']) / 'codex'
    private_directory(home)
    (home / 'config.toml').write_text('admitted profile')
    roles = [{'role': 'review', 'status': 'findings', 'session_id': 'old-review'}]
    state = {'roles': roles, 'iteration': 0, 'cleanup': 'unknown',
             'candidate': {'id': 'a' * 64}, 'checks': {'local': {
                 'state': 'unknown', 'cleanup': 'unknown', 'reason': 'ValueError',
                 'candidate_id': 'a' * 64}}}
    return owned, state, {'state': deepcopy(state)}, gate, deps, resources


def test_observation_is_read_only_and_does_not_relabel_unknown_history(stopped_check):
    owned, state, prior, _, _, resources = stopped_check
    before = resources.manifest.read_bytes()
    proof = observe(owned, state, prior)
    assert proof['state'] == 'observed-quiescent-prelaunch'
    assert proof['historical_cleanup'] == state['cleanup'] == 'unknown'
    assert resources.manifest.read_bytes() == before


@pytest.mark.parametrize('drift', ['data', 'launch', 'role', 'monitoring', 'live', 'failed-check',
                                 'profile-symlink'])
def test_prelaunch_observation_rejects_possible_execution_or_changed_assessment(
    stopped_check, monkeypatch, drift,
):
    owned, state, prior, gate, deps, resources = stopped_check
    if drift == 'data':
        (gate / 'worker/.venv').mkdir()
    elif drift == 'launch':
        (Path(owned['state_dir']) / 'checks/0' / deps['id'] / 'native').mkdir(parents=True)
    elif drift == 'role':
        state['roles'].append({'role': 'review', 'session_id': 'new-review'})
    elif drift == 'profile-symlink':
        profile = (Path(owned['state_dir']) / 'transient/checks/checks/0' / deps['id']
                   / 'codex/config.toml')
        target = profile.with_name('replacement.toml')
        profile.rename(target)
        profile.symlink_to(target)
    else:
        journal = Path(owned['state_dir']) / 'checks/0/prerequisite/native/native-process.json'
        value = {'phase': 'finished', 'monitoring_complete': drift != 'monitoring',
                 'owned': {'999': {'identity': 'same'}}, 'ports': [],
                 'result': {'cleanup': 'observed-native-confirmed', 'exit_code': 1}}
        write_private(journal, value)
        resources.process(journal)
        if drift == 'live':
            monkeypatch.setattr('devflow_temporal.delivery_check_prelaunch.process_table',
                                lambda: {999: {'identity': 'same', 'stat': 'S'}})
        if drift == 'failed-check':
            owned['policy']['checks'] = [{'id': 'prerequisite'}]
    with pytest.raises(ValueError):
        observe(owned, state, prior)
