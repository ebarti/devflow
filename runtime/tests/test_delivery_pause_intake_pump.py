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
