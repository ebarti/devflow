from __future__ import annotations

import subprocess
import json
import shutil
import sys
from pathlib import Path

import pytest
from test_delivery_native import native_configuration as native_configuration
from test_delivery_intake import intake_fixture as intake_fixture

from devflow_temporal.delivery_check_evidence import junit_counts, retain_artifacts


@pytest.mark.parametrize('outcome', ['pass', 'fail', 'skip'])
def test_actual_node_case_outcomes_are_not_inferred_from_exit_or_log(tmp_path, outcome):
    script = tmp_path / 'report.test.mjs'
    script.write_text('import test from "node:test"; test("actual case",'
                      + ('{skip:true},' if outcome == 'skip' else '')
                      + '() => {' + ('throw Error("measured failure");' if outcome == 'fail' else '')
                      + '});\n')
    folder = tmp_path / 'check'
    source = folder / 'pytest-artifacts'
    source.mkdir(parents=True)
    result = subprocess.run(['node', '--test', '--test-reporter=junit',
                             '--test-reporter-destination=' + str(source / 'junit.xml'),
                             str(script)], capture_output=True)
    assert result.returncode == (1 if outcome == 'fail' else 0)
    counts = junit_counts(retain_artifacts(folder, {'id': 'candidate'}), 'candidate', tmp_path)
    assert counts['tests'] == 1
    assert counts['passed'] == (1 if outcome == 'pass' else 0)
    assert counts['failures'] == (1 if outcome == 'fail' else 0)
    assert counts['skipped'] == (1 if outcome == 'skip' else 0)


@pytest.mark.parametrize('content', [None, b'<broken>', b'<html/>',
                                   b'<!DOCTYPE testsuites [<!ENTITY a "entity">]><testsuites/>'])
def test_report_absence_or_malformed_output_cannot_satisfy_a_gate(tmp_path, content):
    source = tmp_path / 'pytest-artifacts'
    source.mkdir()
    if content is not None:
        (source / 'junit.xml').write_bytes(content)
    reference = retain_artifacts(tmp_path, {'id': 'candidate'})
    with pytest.raises(ValueError):
        junit_counts(reference, 'candidate', tmp_path)


def test_empty_summary_is_not_executed_cases_and_report_is_hash_bound(tmp_path):
    source = tmp_path / 'pytest-artifacts'
    source.mkdir()
    (source / 'junit.xml').write_text('<testsuites tests="163"/>')
    reference = retain_artifacts(tmp_path, {'id': 'candidate'})
    assert junit_counts(reference, 'candidate', tmp_path)['passed'] == 0
    report = tmp_path / 'artifacts/junit.xml'
    report.write_text('<testsuites><testcase name="fabricated"/></testsuites>')
    with pytest.raises(ValueError, match='changed'):
        junit_counts(reference, 'candidate', tmp_path)


@pytest.mark.skipif(sys.platform != 'darwin', reason='actual macOS native check lifecycle required')
@pytest.mark.parametrize('outcome', ['pass', 'fail', 'skip'])
def test_actual_broker_enforces_delegated_report_and_retains_it_after_cleanup(
    native_configuration, outcome,
):
    from devflow_temporal.delivery_broker import DeliveryBroker, _git
    from devflow_temporal.delivery_preparation import prepare_authority
    from devflow_temporal.delivery_resources import RunResources
    from devflow_temporal.delivery_store import DeliveryStore

    config, request = native_configuration
    repo = config.raw['repositories']['fixture']
    source = Path(repo['source_path'])
    (source / 'scripts').mkdir()
    node = str(Path(shutil.which('node')).resolve())
    argv = [node, '--test', '--test-reporter=junit',
            '--test-reporter-destination={report_path}', 'scripts/report.test.mjs']
    (source / 'scripts/checks.toml').write_text(
        'schema_version=1\n[checks.scripts]\nkind="junit"\nargv=' + json.dumps(argv) + '\n')
    (source / 'scripts/report.test.mjs').write_text('import test from "node:test";'
        + 'test("measured case",' + ('{skip:true},' if outcome == 'skip' else '')
        + '() => {' + ('throw Error("actual failure");' if outcome == 'fail' else '') + '});')
    _git(source, 'add', '.')
    _git(source, 'commit', '-qm', 'Owned report recipe fixture')
    repo['expected_base_sha'] = _git(source, 'rev-parse', 'HEAD')
    config.raw['execution_mode'] = 'trusted-local'
    config.path.write_text(json.dumps(config.raw))
    request['accepted_plan'] = json.dumps({'scope': 'README', 'steps': ['Inspect README'],
        'verification': ['Run the scripts recipe from scripts/checks.toml with owned JUnit.'],
        'acceptance': ['Executed tests and retained report']})
    store = DeliveryStore(config)
    store.submit(request)
    spec = prepare_authority(store, store.spec(request['run_id']))
    broker = DeliveryBroker(store, spec)
    candidate = broker.prepare()['candidate']
    result = broker.run_checks(0, candidate)
    assert result['state'] == ('passed' if outcome == 'pass' else 'failed')
    report = result['results'][-1]
    assert report['plan_provenance']['recipe'] == 'checks.scripts'
    assert report['test_count'] == (1 if outcome == 'pass' else 0)
    assert report['junit']['tests'] == 1
    assert report['junit']['skipped'] == (1 if outcome == 'skip' else 0)
    assert report['artifacts']['count'] == 1
    if outcome == 'fail':
        assert report['evidence_failure'] == 'required JUnit report records failed test cases'
    store.project(spec['run_id'], phase='blocked', execution_state='blocked', event_type='blocked',
                  message='controlled report fixture', checks={'local': result},
                  iteration=0, outcome='blocked', cleanup='none')
    assert RunResources(spec).finalize('blocked')['resource_cleanup'] == 'confirmed'
    assert junit_counts(report['artifacts'], candidate['id'], Path(spec['state_dir'])) == report['junit']
