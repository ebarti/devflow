from __future__ import annotations

import json
from pathlib import Path

import pytest
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker


@pytest.mark.parametrize('retained_environment', [False, True])
def test_controller_prepares_real_locked_python_before_native_probe(service, tmp_path,
                                                                  retained_environment):
    import subprocess
    import sys

    from test_delivery_store import _git

    from devflow_temporal.delivery_store import DeliveryStore

    store, request = service
    source = Path(store.config.raw['repositories']['fixture']['source_path'])
    project = source / 'worker'
    project.mkdir()
    (project / 'pyproject.toml').write_text(
        '[project]\nname="probe-fixture"\nversion="0.0.1"\nrequires-python=">=3.12"\n'
        '[project.optional-dependencies]\ndev=[]\n')
    (project / 'test_probe.py').write_text('def test_case():\n    assert 1 == 1\n')
    (project / '.gitignore').write_text('.venv/\n')
    subprocess.run(['uv', 'lock', '--python', sys.executable], cwd=project,
                   env={'PATH': __import__('os').environ['PATH']},
                   capture_output=True, check=True)
    _git(source, 'add', 'worker')
    _git(source, 'commit', '-qm', 'test: locked probe fixture')
    store.config.raw['repositories']['fixture']['expected_base_sha'] = _git(
        source, 'rev-parse', 'HEAD')
    store.config.path.write_text(json.dumps(store.config.raw))
    store = DeliveryStore(store.config)
    store.submit({**request, 'base_ref': 'HEAD'})
    spec = store.spec(request['run_id'])
    spec.update(accepted_plan=json.dumps({'verification': ['Run pytest worker/test_probe.py']}),
                verification_test_paths=['worker/test_probe.py'])
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    retained = broker.checkout / 'worker/.venv'
    if retained_environment:
        retained.mkdir()
        (retained / 'original.txt').write_text('Retained original environment data\n')
        original_identity = retained.stat().st_ino
    before = broker.candidate()
    result = broker.run_implementation_preparation(0, before)
    assert result['state'] == 'passed'
    if retained_environment:
        assert retained.stat().st_ino == original_identity
        assert (retained / 'original.txt').read_text() == 'Retained original environment data\n'
        assert not (retained / 'bin/python').exists()
        python = Path(result['python_interpreters'][0]['interpreter'])
        assert python.is_relative_to(Path(spec['state_dir']) / 'transient/implementation-python')
        assert str(python) in result['diagnostic']
    else:
        python = broker.checkout / 'worker/.venv/bin/python'
    measured = subprocess.run([str(python), '-c',
        'import sys,test_probe; test_probe.test_case(); '
        'print(sys.prefix); print("normally imported probe")'],
        cwd=broker.checkout / 'worker', capture_output=True, text=True, check=True)
    assert str(python.parent.parent) in measured.stdout
    assert 'normally imported probe' in measured.stdout
    assert broker.candidate() == before
    assert len(result['results']) == 1
    assert result['results'][0]['argv'][1:3] == ['sync', '--locked']
    assert '--no-install-project' not in result['results'][0]['argv']
    if retained_environment:
        from devflow_temporal.delivery_role_evidence import allocate, read_context
        role = {'spec': spec, 'role': 'implement', 'iteration': 0,
                'workspace': str(broker.checkout), 'candidate': before,
                'evidence_context': {'implementation_preparation': result}}
        enriched = allocate(role, '7' * 64)
        context = read_context(enriched)['implementation_preparation']
        assert context['python_interpreters'] == result['python_interpreters']
        assert context['diagnostic'] == result['diagnostic']


@pytest.fixture
def dependency_handoff(service, monkeypatch):
    """Real receipt producer/store/checkpoints; modeled GitHub transport and role verdicts."""
    import sys
    from copy import deepcopy

    from test_delivery_feature_execution import feature_service
    from test_delivery_feature_merge import Remote
    from test_delivery_store import _git

    from devflow_temporal.delivery_feature_activities import finish_worker
    from devflow_temporal.delivery_feature_execution import (
        continue_feature,
        register_worker,
        registry,
        worker_key,
        worker_spec,
    )
    from devflow_temporal.delivery_github_contract import GitHubDelivery, ordered_chunks

    store, supplied, _ = feature_service(service, monkeypatch)
    plan = json.loads(supplied['accepted_plan'])
    final = plan['workstreams'][0]['chunks'].pop()
    final['depends_on'] = ['client']
    plan['workstreams'][1]['chunks'][0]['depends_on'] = ['model']
    plan['workstreams'].append({'id': 'final', 'title': 'Final comparison', 'issue_number': 12,
                               'acceptance': ['Compare against baseline'], 'chunks': [final]})
    supplied['accepted_plan'] = json.dumps(plan)
    store.submit(supplied)
    parent = store.effective_spec(supplied['run_id'])
    shared, token = registry(parent), parent['feature_delivery']['owner']
    issues = {stream['id']: {'id': 'I_' + str(stream['issue_number']),
              'number': stream['issue_number'], 'url':
              'https://github.com/example/fixture/issues/' + str(stream['issue_number'])}
              for stream in plan['workstreams']}
    record = {'comment_id': 11, 'comment_node_id': 'IC_11', 'manifest': {
        'version': 1, 'issue_id': 'I_feature', 'repository_id': 'R_fixture',
        'revision': 1, 'creation_key': 'create-feature', 'plan': plan,
        'workstream_issues': issues, 'publication': {'stack_id': None, 'members': []}}}
    for key, value in [('accepted-plan', {'digest': digest(plan)}),
                       ('github-record', {'comment_id': 11, 'comment_node_id': 'IC_11'}),
                       ('workstream-issues', issues)]:
        shared.checkpoint(token, key, value)
    remote = Remote(record)
    monkeypatch.setattr(GitHubDelivery, 'api', remote.api)
    # These are the original immutable worker inputs, before coordinator continuation.
    ancestor, immediate, receiver = [worker_spec(parent, chunk, issues[chunk['workstream_id']],
        kind=kind, base_sha=parent['base_sha'], base_branch='main') for chunk, kind in
        zip(ordered_chunks(plan), ['chunk', 'chunk', 'build'], strict=True)]
    sources = [ancestor, immediate]
    for source in sources:
        measurement = source['feature_worker']['chunk_id'] + ' prerequisite measurement: 42'
        source['policy']['checks'] = [{'id': 'measurement', 'argv': [sys.executable, '-c',
            'print(' + repr(measurement) + ')']}]
        source['policy_digest'] = digest(source['policy'])
    def assign(spec):
        register_worker(store, parent, spec)
        worker = spec['feature_worker']
        shared.checkpoint(token, f"assignment:{worker['chunk_id']}:{worker['kind']}", {
            'run_id': spec['run_id'], 'store_path': str(store.config.tracking_db),
            'chunk_id': worker['chunk_id'], 'kind': worker['kind']})
        broker = DeliveryBroker(store, spec)
        broker.prepare()
        (broker.checkout / 'README.md').write_text(worker['chunk_id'] + ' contribution\n')
        _git(broker.checkout, 'commit', '-am', 'feat: original worker', '--signoff')
        return broker

    source_checks = {}
    for index, source in enumerate(sources):
        number = 20 + index
        producer = assign(source)
        candidate = producer.candidate()
        local = producer.run_checks(0, candidate)
        assert local['state'] == 'passed' and len(local['results']) == 1
        checks = {'local': local, **{stage: {'state': 'passed', 'candidate_id': candidate['id']}
                                   for stage in ('prepublish', 'review', 'qa')}}
        pr = {'number': number, 'url': 'https://github.com/example/fixture/pull/' + str(number),
              'head': candidate['head'], 'state': 'OPEN', 'base': source['base_sha']}
        record['manifest']['publication']['members'].append({
            **{key: pr[key] for key in ('number', 'url', 'head')},
            'chunk_id': source['feature_worker']['chunk_id'], 'branch': source['branch'],
            'base_branch': sources[index - 1]['branch'] if index else 'main'})
        record['manifest']['publication']['stack_id'] = 42 if index else None
        with store._connect() as db:
            db.execute("INSERT INTO delivery_effects(effect_key,run_id,kind,request_json,state,"
                       "observed_json,updated_at) VALUES (?,?,'publish',?,'complete',?,?)",
                       ('publish-' + source['run_id'], source['run_id'],
                        json.dumps({'iteration': 0, 'input_candidate_id': candidate['id']}),
                        json.dumps(pr), '2026-10-16T00:00:00+00:00'))
        store.project(source['run_id'], phase='delivered', execution_state='terminal',
            event_type='delivered', message='Fixture prerequisite completed', candidate=candidate,
            pull_request=pr, checks=checks, iteration=0, protocol_revision=10,
            outcome='delivered', cleanup='confirmed', error=None)
        # Use the real completion producer for the verified checkpoint and registry retirement.
        finish_worker(parent, source['run_id'])
        source_checks[source['run_id']] = checks
    assign(receiver)
    shared.finish_worker(token, worker_key(receiver['run_id'], token), {'cleanup': 'confirmed'})
    shared.stop(token, 'stopped-original-coordinator', {})
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='cancelled',outcome='cancelled',"
                   "execution_state='terminal' WHERE run_id=?", (parent['run_id'],))
        store.state.release_work(db, parent['work_id'], 'external:devflow:' + parent['run_id'])
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_k: {'result': {}})
    continued = continue_feature(store, parent['run_id'], {
        'command_id': 'continue-for-dependent-worker', 'expected_revision': 1})
    current = store.effective_spec(continued['run_id'])
    shared.reserve_worker(current['feature_delivery']['owner'],
                          worker_key(receiver['run_id'], current['feature_delivery']['owner']),
                          receiver['feature_worker']['workstream_id'])
    request = {'spec': receiver, 'role': 'implement', 'iteration': 1,
               'workspace': receiver['checkout'],
               'candidate': DeliveryBroker(store, receiver).candidate(),
               'resume_session': 'original-dependent-session', 'findings': ['Compare prerequisite']}
    return store, request, sources, source_checks, record, deepcopy(parent)


@pytest.mark.parametrize('drift', ['missing', 'unfinished', 'changed-checks', 'unrelated'])
def test_dependency_handoff_refuses_unbound_or_uncompleted_sources(dependency_handoff, drift):
    from devflow_temporal.delivery_role_evidence import allocate

    store, request, sources, _, _, _ = dependency_handoff
    source = sources[0]  # The immediate prerequisite stays verified; refuse its broken ancestor.
    if drift in {'missing', 'unrelated'}:
        from devflow_temporal.delivery_feature_execution import registry

        with registry(request['spec']).connect() as db:
            key = 'verified:model' if drift == 'missing' else 'assignment:model:chunk'
            db.execute('DELETE FROM execution_checkpoints WHERE checkpoint_key=?', (key,))
    else:
        with store._connect() as db:
            if drift == 'unfinished':
                db.execute("UPDATE delivery_runs SET outcome='blocked' WHERE run_id=?",
                           (source['run_id'],))
            else:
                db.execute("UPDATE delivery_runs SET checks_json='{}' WHERE run_id=?",
                           (source['run_id'],))
    with pytest.raises(ValueError, match='dependency'):
        allocate(request, '8' * 64, store=store)
