"""In-memory lifecycle tests; SDK no-start proof remains the adapter's responsibility."""
import json
import sqlite3
import uuid
from contextlib import nullcontext

import pytest

from devflow_temporal import delivery_pause as pause
from devflow_temporal import delivery_pause_intake as intake
from devflow_temporal import delivery_pause_intake_pump as pump


class MemoryStore:
    def __init__(self):
        self.uri = f'file:pause-pump-{uuid.uuid4().hex}?mode=memory&cache=shared'
        self.anchor = self._connect()
        self.anchor.execute('''CREATE TABLE delivery_runs (run_id TEXT PRIMARY KEY,
            revision INTEGER, request_json TEXT, execution_state TEXT, outcome TEXT)''')
        self.anchor.execute('CREATE TABLE delivery_outbox (run_id TEXT PRIMARY KEY, state TEXT)')
        pause.initialize(self.anchor)
        self.anchor.commit()

    def _connect(self):
        db = sqlite3.connect(self.uri, uri=True, timeout=1)
        db.row_factory = sqlite3.Row
        return db

    def add(self, root, marker=1):
        spec = {'run_id': root, 'pause_intake_version': marker}
        with self._connect() as db:
            db.execute('INSERT INTO delivery_runs VALUES (?,1,?,?,NULL)',
                       (root, json.dumps(spec), 'queued'))
            db.execute("INSERT INTO delivery_outbox VALUES (?,'pending')", (root,))
            db.execute("INSERT INTO delivery_pause_control VALUES (?, 'running',0,0,?, '',1,"
                       "NULL,?)", (root, 'delivery-' + root, pause._now()))
        return spec

    def control(self, root):
        with self._connect() as db:
            return pause._row(db, root)


@pytest.fixture
def store(monkeypatch):
    store = MemoryStore()
    monkeypatch.setattr(pause, 'root_mutex', lambda *_: nullcontext())
    monkeypatch.setattr(intake, '_spec', lambda *_: {})
    yield store
    store.anchor.close()


def install_controlled_sdk(monkeypatch):
    calls = []
    receipt = {'kind': 'unstarted_intake', 'resumable': True, 'checkpoint_id': 'intake:proof',
               'manifest_sha256': 'proof', 'local_custody': 'confirmed', 'remote_uncertain': []}

    async def seal(store, root, epoch, revision, *, client=None):
        calls.append(('seal', root, epoch, revision, client))
        with store._connect() as db:
            row = pause._row(db, root)
            if row['epoch'] != epoch or row['state'] != 'pausing':
                raise ValueError('stale epoch')
            if row['checkpoint_json'] is None:
                db.execute('UPDATE delivery_pause_control SET checkpoint_json=?, '
                           'revision=revision+1 WHERE root_id=?', (json.dumps(receipt), root))
            elif json.loads(row['checkpoint_json']) != receipt:
                raise ValueError('changed receipt')
        return receipt

    async def activate(store, root, epoch, revision, *, client=None):
        calls.append(('activate', root, epoch, revision, client))
        with store._connect() as db:
            row = pause._row(db, root)
            if row['state'] != 'resuming' or row['epoch'] != epoch:
                raise ValueError('stale resume')
            db.execute('UPDATE delivery_pause_intervals SET resumed_at=? '
                       'WHERE root_id=? AND epoch=?',
                       (pause._now(), root, epoch))
            db.execute("UPDATE delivery_pause_control SET state='running', revision=revision+1 "
                       "WHERE root_id=?", (root,))
            return pause._row(db, root)

    monkeypatch.setattr(intake, 'seal_with_sdk', seal)
    monkeypatch.setattr(intake, 'activate_with_sdk', activate)
    return calls


@pytest.mark.asyncio
async def test_resume_requires_explicit_intent_and_reauthenticates_checkpoint(store, monkeypatch):
    store.add('root')
    calls = install_controlled_sdk(monkeypatch)
    pause.request_pause(store, 'root', 'pause', 0, intake_only=True)
    assert (await pump.advance(store, 'root'))['state'] == 'pausing'
    assert (await pump.advance(store, 'root'))['state'] == 'pausing'
    assert (await pump.advance(store, 'root'))['state'] == 'paused'
    assert [call[0] for call in calls] == ['seal', 'seal']
    paused = store.control('root')
    assert (await pump.advance(store, 'root'))['state'] == 'idle'
    assert store.control('root') == paused
    pause.begin_resume(store, 'root', 'resume', paused['revision'], 1)
    assert (await pump.advance(store, 'root')) == {'root_id': 'root', 'epoch': 1,
                                                   'state': 'running'}
    assert [call[0] for call in calls] == ['seal', 'seal', 'activate']
    assert (await pump.advance(store, 'root'))['state'] == 'idle'
    with store._connect() as db:
        interval = db.execute('SELECT * FROM delivery_pause_intervals WHERE root_id=?',
                              ('root',)).fetchone()
        assert interval['resumed_at'] and db.execute(
            'SELECT count(*) FROM delivery_pause_actors').fetchone()[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('stage', ['begin', 'seal', 'publish', 'activate'])
async def test_lost_transition_ack_recovers_from_durable_state(store, monkeypatch, stage):
    store.add('root')
    calls = install_controlled_sdk(monkeypatch)
    pause.request_pause(store, 'root', 'pause', 0, intake_only=True)
    if stage == 'begin':
        original = pump._begin

        def lost(*args):
            original(*args)
            raise OSError('lost acknowledgement')

        monkeypatch.setattr(pump, '_begin', lost)
        assert (await pump.advance(store, 'root'))['state'] == 'pending'
        monkeypatch.setattr(pump, '_begin', original)
    else:
        assert (await pump.advance(store, 'root'))['state'] == 'pausing'
    if stage == 'seal':
        original = intake.seal_with_sdk

        async def lost(*args, **kwargs):
            await original(*args, **kwargs)
            raise OSError('lost acknowledgement')

        monkeypatch.setattr(intake, 'seal_with_sdk', lost)
        assert (await pump.advance(store, 'root'))['state'] == 'pending'
        monkeypatch.setattr(intake, 'seal_with_sdk', original)
    else:
        assert (await pump.advance(store, 'root'))['state'] == 'pausing'
    if stage == 'publish':
        original = pause.publish_paused

        def lost(*args):
            original(*args)
            raise OSError('lost acknowledgement')

        monkeypatch.setattr(pause, 'publish_paused', lost)
        assert (await pump.advance(store, 'root'))['state'] == 'pending'
        monkeypatch.setattr(pause, 'publish_paused', original)
        assert (await pump.advance(store, 'root'))['state'] == 'idle'
    else:
        assert (await pump.advance(store, 'root'))['state'] == 'paused'
    paused = store.control('root')
    assert paused['state'] == 'paused'
    pause.begin_resume(store, 'root', 'resume', paused['revision'], 1)
    if stage == 'activate':
        original = intake.activate_with_sdk

        async def lost(*args, **kwargs):
            await original(*args, **kwargs)
            raise OSError('lost acknowledgement')

        monkeypatch.setattr(intake, 'activate_with_sdk', lost)
        assert (await pump.advance(store, 'root'))['state'] == 'pending'
        monkeypatch.setattr(intake, 'activate_with_sdk', original)
        assert (await pump.advance(store, 'root'))['state'] == 'idle'
    else:
        assert (await pump.advance(store, 'root'))['state'] == 'running'
    assert store.control('root')['state'] == 'running'
    assert [call[0] for call in calls].count('activate') == 1
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_pause_intervals').fetchone()[0] == 1


@pytest.mark.asyncio
async def test_unknown_sdk_result_stays_fenced_and_next_pass_retries(store, monkeypatch):
    store.add('root')
    install_controlled_sdk(monkeypatch)
    pause.request_pause(store, 'root', 'pause', 0, intake_only=True)
    await pump.advance(store, 'root')
    original = intake.seal_with_sdk

    async def unavailable(*args, **kwargs):
        raise TimeoutError('unknown SDK outcome')

    monkeypatch.setattr(intake, 'seal_with_sdk', unavailable)
    result = await pump.advance(store, 'root')
    assert result['state'] == 'pending' and result['reason_type'] == 'TimeoutError'
    assert store.control('root')['state'] == 'pausing'
    assert store.control('root')['checkpoint_json'] is None
    diagnostic = pump.pending_reason(store, 'root')
    assert diagnostic['epoch'] == 1 and diagnostic['reason_type'] == 'TimeoutError'
    assert 'remains fenced' in diagnostic['reason']
    monkeypatch.setattr(intake, 'seal_with_sdk', original)
    assert (await pump.advance(store, 'root'))['state'] == 'pausing'
    assert pump.pending_reason(store, 'root') is None
    assert (await pump.advance(store, 'root'))['state'] == 'paused'


@pytest.mark.asyncio
async def test_starting_actor_cannot_enter_intake_settlement(store, monkeypatch):
    store.add('root')
    pause.request_pause(store, 'root', 'pause', 0, intake_only=True)

    def racing_spec(*_args):
        with store._connect() as db:
            db.execute("INSERT INTO delivery_pause_actors VALUES "
                       "('root','workflow','run',1,'root')")
        return {}

    monkeypatch.setattr(intake, '_spec', racing_spec)
    result = await pump.advance(store, 'root')
    assert result['state'] == 'pending' and result['reason_type'] == 'ValueError'
    assert store.control('root')['state'] == 'pause_requested'


@pytest.mark.asyncio
async def test_marker_requires_exact_integer_and_bounded_paging(store):
    for root, marker in [('a', 1), ('b', True), ('c', 1), ('d', 2)]:
        store.add(root, marker)
        pause.request_pause(store, root, 'pause-' + root, 0, intake_only=True)
    first = await pump.pump(store, limit=1)
    assert first['results'][0]['root_id'] == 'a' and first['next_cursor'] == 'a'
    second = await pump.pump(store, limit=1, after_root_id=first['next_cursor'])
    assert second['results'][0]['root_id'] == 'c' and second['next_cursor'] == 'c'
    assert (await pump.pump(store, limit=1, after_root_id='c'))['results'] == []
    assert (await pump.advance(store, 'b'))['state'] == 'pending'
    with pytest.raises(ValueError):
        await pump.pump(store, limit=True)


@pytest.mark.asyncio
async def test_dual_marker_does_not_advance_and_stale_reason_is_not_visible(store):
    spec = store.add('root')
    spec['pause_control_version'] = 1
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET request_json=? WHERE run_id=?',
                   (json.dumps(spec), 'root'))
    pause.request_pause(store, 'root', 'pause', 0, intake_only=True)
    result = await pump.advance(store, 'root')
    assert result['state'] == 'pending' and result['reason_type'] == 'ValueError'
    assert store.control('root')['state'] == 'pause_requested'
    pump._record_status(store, 'root', 1, 'controlled pending', 'ValueError')
    assert pump.pending_reason(store, 'root')['reason'] == 'controlled pending'
    with store._connect() as db:
        db.execute('UPDATE delivery_pause_control SET epoch=2 WHERE root_id=?', ('root',))
    assert pump.pending_reason(store, 'root') is None


@pytest.mark.asyncio
async def test_frozen_spec_change_during_selection_does_not_advance(store, monkeypatch):
    store.add('root')
    pause.request_pause(store, 'root', 'pause', 0, intake_only=True)

    def changing_spec(*_args):
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET request_json='{}' WHERE run_id='root'")
        return {}

    monkeypatch.setattr(intake, '_spec', changing_spec)
    result = await pump.advance(store, 'root')
    assert result['state'] == 'pending' and result['reason_type'] == 'ValueError'
    assert store.control('root')['state'] == 'pause_requested'


@pytest.mark.asyncio
async def test_missing_pump_schema_cannot_change_control_or_create_schema(store):
    store.add('root')
    pause.request_pause(store, 'root', 'pause-without-schema', 0, intake_only=True)
    with store._connect() as db:
        db.execute('DROP TABLE delivery_pause_intake_pump_status')
        before = db.execute('PRAGMA schema_version').fetchone()[0]
    result = await pump.advance(store, 'root')
    assert result['state'] == 'pending'
    assert store.control('root')['state'] == 'pause_requested'
    with store._connect() as db:
        assert db.execute('PRAGMA schema_version').fetchone()[0] == before
        assert db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND "
                          "name='delivery_pause_intake_pump_status'").fetchone() is None
