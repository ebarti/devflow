"""Owner-authenticated publication stop evidence; display text grants nothing.

The caller supplies the closed result it has just authenticated through the
existing closure owner. Legacy histories are read explicitly and without sealing
or changing a projection, executor journal, source checkout or admission.
"""
from __future__ import annotations

import asyncio
import hashlib
import lzma
import os
import stat
import zlib
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from datetime import timedelta
from pathlib import Path
from typing import Any

from temporalio.client import Client, WorkflowExecutionStatus, WorkflowHistory

from . import delivery_closures as closures
from .contracts import digest
from .delivery_codec import DELIVERY_DATA_CONVERTER
from .delivery_execution_config import is_observation_mode, observation_mode
from .delivery_lifecycle import RecoveryRefused, require_transition
from .delivery_publication import PUBLICATION_PROOF_VERSION
from .delivery_publication import publication_stop_proof_matches as matches
from .delivery_resources import _ancestors

__all__ = ['matches', 'observe', 'read_closed', 'reduce_history']

VERSION = PUBLICATION_PROOF_VERSION
MAX_HISTORY_EVENTS = 50_000
MAX_HISTORY_BYTES = 64 * 1024 * 1024
MAX_COMPRESSED_BYTES = 32 * 1024 * 1024
MAX_PAYLOAD_BYTES = 4 * 1024 * 1024
HISTORY_SECONDS = 30
_PUBLICATION = frozenset({'delivery_publish', 'delivery_reconcile_publish'})
_SETTLEMENT = frozenset({
    'delivery_project', 'delivery_finalize_resources', 'delivery_terminal_tracker',
    'delivery_feature_stop', 'delivery_feature_settle_workers', 'delivery_feature_settle_effects',
})


def _file_identity(item):
    return (item.st_dev, item.st_ino, item.st_uid, item.st_mode, item.st_nlink,
            item.st_size, item.st_mtime_ns, item.st_ctime_ns)


def _bounded_payloads(payloads, closed):
    """Bound the codec's expanded protobuf bytes before its ordinary decode."""
    total = 0
    for payload in payloads:
        encoding = payload.metadata.get('encoding')
        raw = payload.SerializeToString()
        if encoding in {b'binary/zlib', b'binary/xz'}:
            decoder = (zlib.decompressobj() if encoding == b'binary/zlib'
                       else lzma.LZMADecompressor(memlimit=MAX_HISTORY_BYTES))
            raw = decoder.decompress(payload.data, max_length=MAX_PAYLOAD_BYTES + 1)
            if not decoder.eof or decoder.unused_data:
                _refuse('publication_history_bound_exceeded', closed)
        total += len(raw)
        if total > MAX_PAYLOAD_BYTES:
            _refuse('publication_history_bound_exceeded', closed)


def _refuse(code, closed, *, satisfied=None):
    require_transition({
        'operation': 'recover_publication', 'owner': 'publication_stage', 'owner_proof': True,
        'normalized_lifecycle': 'stopped', 'proof_digest': digest({
            key: closed.get(key) for key in ('workflow_id', 'execution_run_id',
                                            'request_digest', 'recovery_digest', 'result')}),
        'comparisons': [],
        'predicates': [{
            'code': code, 'satisfied': satisfied,
            'missing_or_unknown_fact': 'authenticated original publication-stage stop',
            'next_action': 'inspect or import the exact closed publication history',
        }],
    }, {})


def _binding(spec, closed):
    state = closed.get('result')
    candidate = state.get('candidate') if isinstance(state, dict) else None
    if (not isinstance(state, dict) or state.get('run_id') != spec.get('run_id')
            or state.get('phase') != 'blocked' or state.get('outcome') != 'blocked'
            or state.get('execution_state') != 'blocked'
            or not isinstance(candidate, dict) or not isinstance(candidate.get('id'), str)
            or type(state.get('iteration')) is not int or state['iteration'] < 0
            or not isinstance(closed.get('workflow_id'), str) or not closed['workflow_id']
            or not isinstance(closed.get('execution_run_id'), str)
            or not closed['execution_run_id'] or not closed.get('closed_at')
            or not isinstance(spec.get('request_digest'), str) or not spec['request_digest']
            or closed.get('request_digest') != spec.get('request_digest')):
        _refuse('publication_checkpoint_identity_unproven', closed)
    return {
        'version': VERSION, 'run_id': spec['run_id'], 'workflow_id': closed['workflow_id'],
        'execution_run_id': closed['execution_run_id'], 'request_digest': closed['request_digest'],
        'closed_at': closed['closed_at'], 'recovery_digest': closed.get('recovery_digest'),
        'iteration': state['iteration'],
        'candidate_id': candidate['id'], 'stop_state_sha256': digest(state),
    }


async def _checked_facts(history, closed):
    if (len(history.events) > MAX_HISTORY_EVENTS
            or sum(event.ByteSize() for event in history.events) > MAX_HISTORY_BYTES):
        _refuse('publication_history_bound_exceeded', closed)
    if not history.events:
        _refuse('publication_stage_unproven', closed)
    if (history.events[0].workflow_execution_started_event_attributes.input.ByteSize()
            > MAX_PAYLOAD_BYTES
            or history.events[-1].workflow_execution_completed_event_attributes.result.ByteSize()
            > MAX_PAYLOAD_BYTES):
        _refuse('publication_history_bound_exceeded', closed)
    start = history.events[0].workflow_execution_started_event_attributes
    _bounded_payloads([*start.input.payloads, *start.memo.fields.values()], closed)
    _bounded_payloads(
        history.events[-1].workflow_execution_completed_event_attributes.result.payloads, closed)
    return await closures.closure_facts(history)


async def reduce_history(spec, closed, history):
    """Reduce a bounded authentic history; unknown later activities fail closed."""
    binding = _binding(spec, closed)
    facts = await _checked_facts(history, closed)
    if (any(facts.get(key) != closed.get(key) for key in (
            'workflow_id', 'execution_run_id', 'request_digest', 'recovery_digest', 'closed_at'))
            or facts['status'] != 'COMPLETED' or facts['result'] != closed['result']
            or not facts['inputs'] or not isinstance(facts['inputs'][0], dict)
            or facts['inputs'][0].get('run_id') != spec['run_id']):
        _refuse('publication_history_identity_changed', closed, satisfied=False)
    recovery = facts['inputs'][1] if len(facts['inputs']) > 1 else None
    if (digest(recovery) if recovery is not None else None) != closed.get('recovery_digest'):
        _refuse('publication_history_identity_changed', closed, satisfied=False)
    latest = None
    for event in reversed(history.events):
        if event.HasField('activity_task_scheduled_event_attributes'):
            attributes = event.activity_task_scheduled_event_attributes
            if attributes.activity_type.name not in _SETTLEMENT:
                latest = event
                break
    if latest is None:
        _refuse('publication_stage_unproven', closed)
    attributes = latest.activity_task_scheduled_event_attributes
    if attributes.activity_type.name not in _PUBLICATION:
        _refuse('publication_stage_changed', closed, satisfied=False)
    if attributes.input.ByteSize() > MAX_PAYLOAD_BYTES:
        _refuse('publication_history_bound_exceeded', closed)
    _bounded_payloads(attributes.input.payloads, closed)
    values = await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads)
    request = values[0] if len(values) == 1 else None
    if (not isinstance(request, dict) or request.get('spec') != spec
            or request.get('iteration') != binding['iteration']
            or request.get('candidate') != closed['result']['candidate']):
        _refuse('publication_history_request_changed', closed, satisfied=False)
    outcome = None
    for event in history.events:
        for field, kind in (
            ('activity_task_completed_event_attributes', 'completed'),
            ('activity_task_failed_event_attributes', 'failed'),
            ('activity_task_timed_out_event_attributes', 'timed_out'),
            ('activity_task_canceled_event_attributes', 'cancelled'),
        ):
            if event.HasField(field):
                value = getattr(event, field)
                if value.scheduled_event_id == latest.event_id:
                    outcome = (event.event_id, kind, value)
    if outcome is None:
        _refuse('publication_activity_outcome_unknown', closed)
    event_id, kind, value = outcome
    if kind == 'completed':
        if value.result.ByteSize() > MAX_PAYLOAD_BYTES:
            _refuse('publication_history_bound_exceeded', closed)
        _bounded_payloads(value.result.payloads, closed)
        results = await DELIVERY_DATA_CONVERTER.decode(value.result.payloads)
        result = results[0] if len(results) == 1 else None
        if not isinstance(result, dict) or result.get('state') != 'pending':
            _refuse('publication_already_completed', closed, satisfied=False)
    return {**binding, 'source': 'closed_history',
            'history_sha256': closures.history_sha256(history),
            'scheduled_event_id': latest.event_id, 'outcome_event_id': event_id,
            'activity': attributes.activity_type.name, 'activity_outcome': kind}


def _sealed_history(store, closed):
    """Read the exact content address with byte bounds before decompression."""
    record = closures.sealed_record(
        store, closed['workflow_id'], execution_run_id=closed['execution_run_id'])
    if record is None:
        return None
    if record['event_count'] > MAX_HISTORY_EVENTS:
        _refuse('publication_history_bound_exceeded', closed)
    if record['source'] == 'temporal_snapshot':
        from .delivery_retained_closures import verify_record

        verify_record(store, record)
    root = Path(store.config.state_root) / 'closures'
    _ancestors(root / 'history')
    info = root.lstat()
    if (root.resolve() != root or not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700):
        raise ValueError('sealed publication history directory is not its private owner')
    root_identity = (info.st_dev, info.st_ino, info.st_uid, info.st_mode)
    path = root / (record['history_sha256'] + '.json.xz')
    before = path.lstat()
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
                or _file_identity(info) != _file_identity(before)):
            raise ValueError('sealed publication history is not its private owned file')
        if info.st_size > MAX_COMPRESSED_BYTES:
            _refuse('publication_history_bound_exceeded', closed)
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            compressed = stream.read(MAX_COMPRESSED_BYTES + 1)
        after, named = os.fstat(fd), path.lstat()
        _ancestors(path)
        root_after = root.lstat()
        if (_file_identity(info) != _file_identity(after)
                or _file_identity(after) != _file_identity(named)
                or root_identity != (root_after.st_dev, root_after.st_ino,
                                     root_after.st_uid, root_after.st_mode)):
            raise ValueError('sealed publication history changed during observation')
    finally:
        os.close(fd)
    decoder = lzma.LZMADecompressor(memlimit=MAX_HISTORY_BYTES)
    raw = decoder.decompress(compressed, max_length=MAX_HISTORY_BYTES + 1)
    if len(raw) > MAX_HISTORY_BYTES or not decoder.eof or decoder.unused_data:
        _refuse('publication_history_bound_exceeded', closed)
    if hashlib.sha256(raw).hexdigest() != record['history_sha256']:
        raise ValueError('sealed publication history changed its recorded bytes')
    return WorkflowHistory.from_json(closed['workflow_id'], raw.decode('utf-8'))


async def _history(store, closed):
    sealed = _sealed_history(store, closed)
    if sealed is not None:
        return sealed
    client = await Client.connect(
        store.config.temporal_address,
        namespace=store.config.raw.get('temporal_namespace', 'default'),
        data_converter=DELIVERY_DATA_CONVERTER,
    )
    handle = client.get_workflow_handle(closed['workflow_id'], run_id=closed['execution_run_id'])
    return await _fetch_history(handle, closed)


async def _fetch_history(handle, closed):
    events, size = [], 0
    async for event in handle.fetch_history_events(page_size=20, rpc_timeout=timedelta(seconds=10)):
        size += event.ByteSize()
        if len(events) >= MAX_HISTORY_EVENTS or size > MAX_HISTORY_BYTES:
            _refuse('publication_history_bound_exceeded', closed)
        events.append(event)
    return WorkflowHistory(closed['workflow_id'], events)


def _run(operation, closed):
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(
                copy_context().run,
                lambda: asyncio.run(asyncio.wait_for(operation(), timeout=HISTORY_SECONDS)),
            ).result(timeout=HISTORY_SECONDS + 5)
    except RecoveryRefused:
        raise
    except Exception:
        _refuse('publication_stage_unproven', closed)


def read_closed(store, spec, *, workflow_id=None, preserve=False):
    """Authenticate closure with limits before any result/codec decoding.

    A sealed candidate is bounded before connecting, so an oversized local
    artifact cannot escape through the general closure reader or an outage.
    Live authority is still checked; retention NotFound alone selects the sealed
    fallback. Only execution may explicitly preserve newly authenticated bytes.
    """
    workflow_id = workflow_id or 'delivery-' + spec['run_id']
    context = {'workflow_id': workflow_id, 'request_digest': spec['request_digest']}
    preserve = preserve and not store.read_only and not is_observation_mode()

    async def read():
        record = closures.sealed_record(store, workflow_id)
        cached = cached_facts = None
        if record is not None:
            selected = {**context, 'execution_run_id': record['execution_run_id']}
            cached = _sealed_history(store, selected)
            cached_facts = await _checked_facts(cached, selected)
            if any(cached_facts.get(key) != record.get(key) for key in (
                    'workflow_id', 'execution_run_id', 'status', 'closed_at',
                    'request_digest', 'recovery_digest', 'event_count')):
                _refuse('publication_history_identity_changed', selected, satisfied=False)
        client = await Client.connect(
            store.config.temporal_address,
            namespace=store.config.raw.get('temporal_namespace', 'default'),
            data_converter=DELIVERY_DATA_CONVERTER,
        )
        handle = client.get_workflow_handle(workflow_id)
        try:
            description = await handle.describe()
        except Exception as exc:
            if not closures.not_found(exc) or cached is None:
                raise
            facts, history, fresh = cached_facts, cached, False
        else:
            if (description.status != WorkflowExecutionStatus.COMPLETED
                    or not description.close_time
                    or description.workflow_type != closures.WORKFLOW_TYPE):
                _refuse('publication_closure_not_completed', context, satisfied=False)
            if cached_facts and cached_facts['execution_run_id'] == description.run_id:
                facts, history, fresh = cached_facts, cached, False
            else:
                exact = client.get_workflow_handle(workflow_id, run_id=description.run_id)
                history = await _fetch_history(exact, context)
                facts = await _checked_facts(history, context)
                fresh = True
            if (facts['workflow_id'] != description.id
                    or facts['execution_run_id'] != description.run_id
                    or facts['closed_at'] != description.close_time.isoformat()):
                _refuse('publication_history_identity_changed', context, satisfied=False)
        if facts['status'] != 'COMPLETED':
            _refuse('publication_closure_not_completed', context, satisfied=False)
        closed = {key: facts[key] for key in ('workflow_id', 'execution_run_id', 'closed_at',
                                             'request_digest', 'recovery_digest', 'result')}
        _binding(spec, closed)
        recovery = facts['inputs'][1] if len(facts['inputs']) > 1 else None
        if (not facts['inputs'] or facts['inputs'][0].get('run_id') != spec['run_id']
                or (digest(recovery) if recovery is not None else None)
                != closed['recovery_digest']):
            _refuse('publication_history_identity_changed', closed, satisfied=False)
        return closed, history, facts, fresh

    with observation_mode():
        if not store.owns_execution(spec):
            _refuse('publication_execution_transport_unknown', context)
        closed, history, facts, fresh = _run(read, context)
    if preserve and fresh:
        closures.seal(store, history, facts, run_id=spec['run_id'], source='temporal')
    return closed, history


def observe(store, spec: dict[str, Any], closed: dict[str, Any], *, history=None) -> dict[str, Any]:
    """Build bounded proof from an owner-authenticated closure, without writes."""
    binding = _binding(spec, closed)
    checks = closed['result'].get('checks')
    failure = checks.get('failure') if isinstance(checks, dict) else None
    failure = failure if isinstance(failure, dict) else {}
    if (failure.get('cause_type') == 'publication_deadline'
            and failure.get('classification') == 'transient'
            and failure.get('stage') in {'publishing', 'publishing_pending'}):
        return {**binding, 'source': 'closed_result', 'failure_sha256': digest(failure)}

    async def read():
        observed = history if history is not None else await _history(store, closed)
        return await reduce_history(spec, closed, observed)

    return _run(read, closed)
