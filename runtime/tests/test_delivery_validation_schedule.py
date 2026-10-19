"""Real commands run once at the required candidate, never a fabricated cache pass."""
from __future__ import annotations

import json
import sys

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_validation_schedule import (
    local_checks, prepublication, require_deferred_checks,
)


def recipe(ident, argv=None):
    return {"id": ident, "kind": "check", "argv": argv or ["tool", "check"]}


def spec(pre, post):
    return {"validation_schedule_version": 1,
            "policy": {"prepublish_checks": pre, "checks": post}}


def test_dependency_and_history_checks_stay_before_publication():
    dependency = recipe("install", ["corepack", "pnpm", "install", "/store"])
    git = recipe("diff", ["git", "diff", "--check"])
    before = recipe("broad-before")
    after = recipe("broad-after")
    value = spec([dependency, before, git], [dependency, after, git])
    assert prepublication(value) == ([dependency, git], {"broad-before": "broad-after"})
    require_deferred_checks(value, {"deferred_to_checks": {"broad-before": "broad-after"}},
                            {"results": [{**after, "passed": True}]})
    for changed in [{"results": []}, {"results": [{**after, "passed": False}]},
                    {"results": [{**after, "passed": True, "argv": ["different"]}]}]:
        with pytest.raises(ValueError, match="no passing execution"):
            require_deferred_checks(value,
                                    {"deferred_to_checks": {"broad-before": "broad-after"}},
                                    changed)


@pytest.mark.parametrize("field,value", [("argv", ["different"]), ("cwd", "other"),
    ("min_tests", 2), ("required_selectors", ["different.test.ts"]),
    ("network_domains", ["example.invalid"]), ("timeout_seconds", 42)])
def test_changed_validation_inputs_are_not_deduplicated(field, value):
    before, after = recipe("before"), recipe("after")
    after[field] = value
    assert prepublication(spec([before], [after])) == ([before], {})


def test_unique_prerequisite_or_build_only_worker_keeps_requested_order():
    broad = recipe("broad")
    unique = recipe("consumer", ["node", "consume-build-output.js"])
    assert prepublication(spec([broad, unique], [broad])) == ([broad, unique], {})
    value = spec([broad], [broad])
    value["feature_worker"] = {"kind": "build"}
    assert prepublication(value) == ([broad], {})
    value.pop("feature_worker")
    value.pop("validation_schedule_version")
    assert prepublication(value) == ([broad], {})


def test_actual_broker_executes_shared_broad_command_only_after_publication(service):
    original, request = service
    path = original.config.path
    configured = json.loads(path.read_text())
    counter = original.config.state_root / "validation-count.json"
    code = ("from pathlib import Path; import json; "
            f"p=Path({str(counter)!r}); "
            "p.write_text(json.dumps(json.loads(p.read_text())+1 if p.exists() else 1)); "
            "print('checked')")
    before = recipe("before", [sys.executable, "-c", code])
    after = {**before, "id": "after"}
    configured["repositories"]["fixture"].update(prepublish_checks=[before], checks=[after])
    path.write_text(json.dumps(configured))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(request)
    value = store.spec(request["run_id"])
    broker = DeliveryBroker(store, value)
    candidate = broker.prepare()["candidate"]
    prechecked = broker.run_prechecks(0, candidate)
    assert prechecked["state"] == "passed" and prechecked["results"] == []
    assert prechecked["deferred_to_checks"] == {"before": "after"}
    assert not counter.exists()
    checked = broker.run_checks(0, candidate)
    assert checked["state"] == "passed" and json.loads(counter.read_text()) == 1
    require_deferred_checks(value, prechecked, checked)
    # An actual source change must be tested again, not reused from the first pass.
    (broker.checkout / "README.md").write_text("Changed candidate\n")
    _git(broker.checkout, "add", "README.md")
    _git(broker.checkout, "-c", "commit.gpgsign=false", "-c", "user.name=Fixture",
         "-c", "user.email=fixture@example.invalid", "commit", "-qm", "Changed candidate")
    changed = broker.candidate()
    assert changed["id"] != candidate["id"]
    assert broker.run_checks(1, changed)["state"] == "passed"
    assert json.loads(counter.read_text()) == 2


def static(ident='planned-static-fixture', **extra):
    return {**recipe(ident), 'kind': 'static', 'timeout_seconds': 600,
            'junit_required': False, 'min_tests': 1,
            'plan_provenance': {'recipe': 'checks.fixture', 'base_sha': 'frozen'}, **extra}


def test_planned_static_binds_both_requirements_and_stricter_execution_bounds():
    configured = {**recipe('configured'), 'timeout_seconds': 900,
                  'generated_directories': ['other/.venv']}
    planned = static(generated_directories=['worker/.venv'])
    original = json.dumps([configured, planned], sort_keys=True)
    deps = recipe('planned-python-dependencies-fixture', ['uv', 'sync', '--locked'])
    value = {'validation_schedule_version': 2}
    result = local_checks(value, [configured], [planned, deps])
    assert [c['id'] for c in result] == [deps['id'], 'configured']
    merged = result[1]
    assert merged['satisfies'] == ['configured', planned['id']]
    assert merged['plan_provenance'] == planned['plan_provenance']
    assert merged['timeout_seconds'] == 600
    assert merged['generated_directories'] == ['other/.venv', 'worker/.venv']
    assert json.dumps([configured, planned], sort_keys=True) == original
    # An already stricter configured bound cannot be relaxed.
    assert local_checks(value, [{**configured, 'timeout_seconds': 42}], [planned])[0][
        'timeout_seconds'] == 42
    # Historical reconciliation must still see the original two executions.
    for version in [None, 1]:
        assert local_checks({'validation_schedule_version': version}, [configured],
                            [planned, deps]) == [deps, configured, planned]


@pytest.mark.parametrize('side,field,value', [
    ('planned', 'argv', ['other', 'check']), ('planned', 'cwd', 'other'),
    ('planned', 'kind', 'test'), ('configured', 'kind', 'test'),
    ('planned', 'min_tests', 2), ('planned', 'junit_required', True),
    ('configured', 'junit_required', True),
    ('planned', 'required_selectors', ['file.test.ts']),
    ('planned', 'tracked_recipe', True),
    ('configured', 'reject_regex', 'unacceptable'),
    ('configured', 'environment', {'MODE': 'other'}),
    ('planned', 'network_domains', ['example.invalid']),
    ('configured', 'plan_provenance', {'recipe': 'checks.other'}),
])
def test_static_coalescing_retains_different_evidence_and_authority(side, field, value):
    configured, planned = recipe('configured'), static()
    (planned if side == 'planned' else configured)[field] = value
    assert local_checks({'validation_schedule_version': 2}, [configured], [planned]) == [
        configured, planned]


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
            require_deferred_checks(require, {'deferred_to_checks': {'before': 'configured'}}, checked)
    (broker.checkout / 'README.md').write_text('Changed candidate\n')
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, '-c', 'commit.gpgsign=false', '-c', 'user.name=Fixture',
         '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'Changed candidate')
    changed = broker.candidate()
    assert changed['id'] != candidate['id']
    broker._run_check_list(broker.checkout, checks, broker.evidence_dir / 'checks/1', changed)
    assert json.loads(counter.read_text()) == 2
