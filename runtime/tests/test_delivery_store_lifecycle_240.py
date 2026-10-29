"""Owner preflight uses the same sealed store admission as execution."""


import pytest
from test_delivery_store import _scope_amendment_fixture

pytest_plugins = ["test_delivery_store"]


def _counts(store):
    with store._connect() as db:
        return tuple(
            db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("delivery_commands", "delivery_scope_amendments", "delivery_events")
        )


@pytest.mark.parametrize(
    "field,value", [("expected_revision", 999), ("expected_candidate_id", "f" * 64)]
)
def test_scope_stale_request_never_mutates(service, monkeypatch, field, value):
    store, request = service
    _original, _state, command, _closed = _scope_amendment_fixture(store, request, monkeypatch)
    command[field] = value
    before = _counts(store)
    for preflight in (True, False):
        with pytest.raises(ValueError):
            store.amend_scope("run-1", command, preflight=preflight)
        assert _counts(store) == before


@pytest.mark.asyncio
async def test_default_repair_public_assessment_and_admission_agree(service, monkeypatch):
    import httpx
    from test_delivery_store import _failed_published_repair_fixture

    from devflow_temporal.delivery_api import create_app

    store, request = service
    _spec, state, command = _failed_published_repair_fixture(store, request, monkeypatch)
    app = create_app(store.config.path)
    app.state.delivery.store = store
    host = store.config.dashboard_url
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=('127.0.0.1', 10001)), base_url=host,
    ) as client:
        token = (await client.get('/api/session')).json()['csrf_token']
        headers = {'Origin': host, 'X-Devflow-CSRF': token}
        base = '/api/runs/run-1/'
        observed = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        assert observed.status_code == 200 and observed.json()['eligible']
        stale = {**command, 'expected_revision': command['expected_revision'] + 1}
        preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': stale})
        actual = await client.post(base + 'continue-repair', json=stale, headers=headers)
        assert actual.status_code == 409 and actual.json()['detail'] == preview.json()
        assert preview.json()['blockers'][0]['code'] == 'revision_changed'
        with store._connect() as db:
            original_owner = db.execute('SELECT owner FROM claims WHERE work_id=?',
                                        (request['work_id'],)).fetchone()[0]
            original_repository = db.execute('SELECT repository FROM works WHERE id=?',
                                             (request['work_id'],)).fetchone()[0]
            db.execute("UPDATE claims SET owner='external:devflow:another' WHERE work_id=?",
                       (request['work_id'],))
        owner_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        owner_admission = await client.post(base + 'continue-repair', json=command,
                                            headers=headers)
        assert owner_admission.status_code == 409
        assert owner_admission.json()['detail'] == owner_preview.json()
        assert 'repair_owner_conflict' in {b['code'] for b in owner_preview.json()['blockers']}
        with store._connect() as db:
            assert not db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                                  (command['command_id'],)).fetchone()
            db.execute('UPDATE claims SET owner=? WHERE work_id=?',
                       (original_owner, request['work_id']))
            db.execute("UPDATE works SET repository='github.com/example/other' WHERE id=?",
                       (request['work_id'],))
        work_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        work_admission = await client.post(base + 'continue-repair', json=command,
                                           headers=headers)
        assert work_admission.status_code == 409
        assert work_admission.json()['detail'] == work_preview.json()
        assert 'repair_work_binding_unproven' in {
            b['code'] for b in work_preview.json()['blockers']}
        with store._connect() as db:
            assert not db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                                  (command['command_id'],)).fetchone()
            db.execute('UPDATE works SET repository=? WHERE id=?',
                       (original_repository, request['work_id']))
            db.execute("UPDATE delivery_attempts SET state='running' WHERE role='implement'")
        attempt_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        attempt_admission = await client.post(base + 'continue-repair', json=command,
                                              headers=headers)
        assert not attempt_preview.json()['eligible'], (attempt_preview.json(),
                                                         attempt_admission.json())
        assert attempt_admission.status_code == 409
        assert attempt_admission.json()['detail'] == attempt_preview.json()
        assert 'repair_attempt_custody_unknown' in {
            b['code'] for b in attempt_preview.json()['blockers']}
        with store._connect() as db:
            assert not db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                                  (command['command_id'],)).fetchone()
            db.execute("UPDATE delivery_attempts SET state='finished' WHERE role='implement'")
            db.execute("UPDATE delivery_effects SET state='unknown' WHERE kind='publish'")
        effect_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        effect_admission = await client.post(base + 'continue-repair', json=command,
                                             headers=headers)
        assert not effect_preview.json()['eligible'] and effect_admission.status_code == 409
        assert effect_admission.json()['detail'] == effect_preview.json()
        assert 'repair_effects_unsettled' in {
            b['code'] for b in effect_preview.json()['blockers']}
        with store._connect() as db:
            db.execute("UPDATE delivery_effects SET state='complete' WHERE kind='publish'")
        state['roles'][0]['cleanup'] = 'unknown'
        custody_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        custody_admission = await client.post(base + 'continue-repair', json=command,
                                              headers=headers)
        assert custody_admission.status_code == 409
        assert custody_admission.json()['detail'] == custody_preview.json(), (
            custody_preview.json(), custody_admission.json())
        assert 'repair_role_custody_unknown' in {
            b['code'] for b in custody_preview.json()['blockers']}
        state['roles'][0]['cleanup'] = 'confirmed'
        state['checks']['local'] = {'state': 'unknown'}
        check_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        check_admission = await client.post(base + 'continue-repair', json=command,
                                            headers=headers)
        assert check_admission.status_code == 409
        assert check_admission.json()['detail'] == check_preview.json()
        assert 'repair_check_outcome_unknown' in {
            b['code'] for b in check_preview.json()['blockers']}
        del state['checks']['local']
        accepted = await client.post(base + 'continue-repair', json=command, headers=headers)
        assert accepted.status_code == 200 and accepted.json()['authorized_through_iteration'] == 4
        replay = await client.post(base + 'continue-repair', json=command, headers=headers)
        assert replay.status_code == 200 and replay.json() == accepted.json()
        replay_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        assert replay_preview.json()['disposition'] == 'already_admitted'
        conflict = {**command, 'expected_revision': command['expected_revision'] + 1}
        conflict_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': conflict})
        conflict_admission = await client.post(base + 'continue-repair', json=conflict,
                                               headers=headers)
        assert conflict_admission.status_code == 409
        assert conflict_admission.json()['detail'] == conflict_preview.json()
        assert conflict_preview.json()['blockers'][0]['code'] == 'command_conflict'
        another = {**command, 'command_id': 'another-repair-grant'}
        grant_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': another})
        grant_admission = await client.post(base + 'continue-repair', json=another, headers=headers)
        assert grant_admission.status_code == 409
        assert grant_admission.json()['detail'] == grant_preview.json()
        assert 'repair_grant_consumed' in {b['code'] for b in grant_preview.json()['blockers']}
        with store._connect() as db:
            assert db.execute('SELECT count(*) FROM delivery_commands WHERE command_id=?',
                              (command['command_id'],)).fetchone()[0] == 1
            assert not db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                                  (another['command_id'],)).fetchone()
            db.execute('UPDATE delivery_commands SET response_json=? WHERE command_id=?',
                       ('{}', command['command_id']))
        corrupt_preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        corrupt_admission = await client.post(base + 'continue-repair', json=command,
                                              headers=headers)
        assert corrupt_admission.status_code == 409
        assert corrupt_admission.json()['detail'] == corrupt_preview.json()
        assert 'command_receipt_unknown' in {b['code'] for b in corrupt_preview.json()['blockers']}


@pytest.mark.asyncio
async def test_preplan_stop_refuses_implementation_resume_across_restart(service, monkeypatch):
    """A closed pre-intake preparation failure must not be resumed as implementation."""
    import asyncio
    import copy
    import json
    import socket
    import sys

    import httpx
    import uvicorn

    from devflow_temporal.delivery_api import create_app
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_store import DeliveryStore

    initial, request = service
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen(128)
    raw = copy.deepcopy(initial.config.raw)
    raw['dashboard_url'] = f'http://127.0.0.1:{listener.getsockname()[1]}'
    raw['roles']['intake'] = {'model': 'fixture', 'effort': 'low'}
    initial.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(initial.config.path))
    request = {key: value for key, value in request.items() if key != 'accepted_plan'}
    store.submit(request)
    store.mark_start('run-1', accepted=True)
    error = 'fixture preparation failed before planning'
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=error, outcome='blocked', cleanup='confirmed', error=error,
                  protocol_revision=3, iteration=0)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET workflow_id='delivery-run-1' WHERE run_id='run-1'")
        store.state.release_work(db, request['work_id'], 'external:devflow:run-1')
        row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', ('run-1',)).fetchone()
        saved_request = row['request_json']
    spec = store.submitted_spec('run-1')
    assert not spec['accepted_plan']
    closed = {'workflow_id': 'delivery-run-1', 'execution_run_id': 'preplan-stop',
              'request_digest': row['request_digest'], 'recovery_digest': None,
              'result': {'run_id': 'run-1', 'phase': 'blocked', 'execution_state': 'blocked',
                         'outcome': 'blocked', 'cleanup': 'confirmed', 'error': error,
                         'revision': 3, 'iteration': 0, 'candidate': None,
                         'pull_request': None, 'checks': {}}}
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result', lambda *_a, **_k: closed)
    command = {'continuation_kind': 'stopped_delivery_resume', 'command_id': 'preplan-resume',
               'expected_revision': 3, 'expected_iteration': 0,
               'expected_candidate_id': '0' * 64, 'expected_candidate_head': '0' * 40,
               'additional_iterations': 1}

    # Reopen the durable store to exercise the same boundary after a restart.
    restarted = DeliveryStore(DeliveryConfig.load(initial.config.path))
    app = create_app(restarted.config.path)
    app.state.delivery.store = restarted
    with restarted._connect() as db:
        before = list(db.iterdump())
    host = restarted.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=host) as client:
        token = (await client.get('/api/session')).json()['csrf_token']
        preview = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': 'stopped_delivery_resume', 'request': command})
        response = await client.post('/api/runs/run-1/continue-repair', json=command,
                                     headers={'Origin': host, 'X-Devflow-CSRF': token})
        assert preview.status_code == 200 and not preview.json()['eligible']
        assert 'accepted_plan_unavailable' in {b['code'] for b in preview.json()['blockers']}
        assert response.status_code == 409 and response.json()['detail'] == preview.json()
        expected_assessment = preview.json()
    # Exercise the real read-only CLI and its loopback client against this owned API.
    assessment_file = initial.config.path.parent / 'preplan-assessment.json'
    assessment_file.write_text(json.dumps({'kind': 'stopped_delivery_resume', 'request': command}))
    server = uvicorn.Server(uvicorn.Config(app, lifespan='off', log_level='error'))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(200):
            if server.started:
                break
            await asyncio.sleep(.01)
        assert server.started
        process = await asyncio.create_subprocess_exec(
            sys.executable, '-m', 'devflow_temporal.delivery_control', '--config',
            str(initial.config.path), 'assess-transition', '--id', 'run-1', '--request',
            str(assessment_file), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
        assert process.returncode == 0, stderr.decode()
        assert json.loads(stdout) == expected_assessment
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, 10)
        listener.close()
    with restarted._connect() as db:
        assert list(db.iterdump()) == before
        assert db.execute('SELECT request_json FROM delivery_runs').fetchone()[0] == saved_request
