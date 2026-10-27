"""Real commands run once at the required candidate, never a fabricated cache pass."""
from __future__ import annotations

import json
import sys

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_validation_schedule import (
    local_checks,
    require_deferred_checks,
)


def recipe(ident, argv=None):
    return {"id": ident, "kind": "check", "argv": argv or ["tool", "check"]}


def static(ident='planned-static-fixture', **extra):
    return {**recipe(ident), 'kind': 'static', 'timeout_seconds': 600,
            'junit_required': False, 'min_tests': 1,
            'plan_provenance': {'recipe': 'checks.fixture', 'base_sha': 'frozen'}, **extra}


@pytest.mark.parametrize('argv', [['git', 'diff', '--check'], ['uv', 'sync', '--locked'],
                                 ['corepack', 'pnpm', 'install', '/store']])
def test_history_and_preparation_are_never_coalesced(argv):
    configured, planned = recipe('configured', argv), static(argv=argv)
    assert local_checks({'validation_schedule_version': 2}, [configured], [planned]) == [
        configured, planned]


@pytest.mark.parametrize('fails', [False, True])
def test_real_static_command_runs_once_with_real_verdict_and_again_on_new_candidate(service, fails):
    store, request = service
    store.submit(request)
    value = store.spec(request['run_id'])
    assert value['validation_schedule_version'] == 2
    broker = DeliveryBroker(store, value)
    candidate = broker.prepare()['candidate']
    counter = store.config.state_root / 'static-validation-count.json'
    code = ('from pathlib import Path; import json; '
            f'p=Path({str(counter)!r}); '
            'p.write_text(json.dumps(json.loads(p.read_text())+1 if p.exists() else 1)); '
            f'print("checked"); raise SystemExit({1 if fails else 0})')
    configured = recipe('configured', [sys.executable, '-c', code])
    planned = static(argv=configured['argv'])
    checks = local_checks(value, [configured], [planned])
    checked = broker._run_check_list(broker.checkout, checks,
                                    broker.evidence_dir / 'checks/0', candidate)
    assert checked['state'] == ('failed' if fails else 'passed')
    assert json.loads(counter.read_text()) == 1 and len(checked['results']) == 1
    execution = checked['results'][0]
    assert execution['passed'] is not fails
    assert execution['satisfies'] == ['configured', planned['id']]
    assert execution['plan_provenance'] == planned['plan_provenance']
    assert execution['started_at'] <= execution['finished_at']
    assert execution['duration_seconds'] > 0
    require = {'validation_schedule_version': 2,
               'policy': {'prepublish_checks': [{**configured, 'id': 'before'}],
                          'checks': [configured]}}
    if not fails:
        require_deferred_checks(require, {'deferred_to_checks': {'before': 'configured'}}, checked)
    else:
        with pytest.raises(ValueError, match='no passing execution'):
            require_deferred_checks(
                require, {"deferred_to_checks": {"before": "configured"}}, checked
            )
    (broker.checkout / 'README.md').write_text('Changed candidate\n')
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, '-c', 'commit.gpgsign=false', '-c', 'user.name=Fixture',
         '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'Changed candidate')
    changed = broker.candidate()
    assert changed['id'] != candidate['id']
    broker._run_check_list(broker.checkout, checks, broker.evidence_dir / 'checks/1', changed)
    assert json.loads(counter.read_text()) == 2
