"""The exact terminal item supplies completed native scope authority."""
from __future__ import annotations

import asyncio
import copy
import json

import pytest
from test_delivery_blocked_scope import admitted, rewrite_observation
from test_delivery_blocked_scope import blocked_scope as blocked_scope
from test_delivery_store import service as service

from devflow_temporal.delivery_activities import delivery_repair_preflight
from devflow_temporal.delivery_resources import read_private


def two_finals(case, *, earlier_json=False):
    observation = read_private(case.folder / 'native-thread-observation.json')
    final = copy.deepcopy(observation['turns'][-1]['items'][-1])
    earlier = {**final, 'id': 'earlier-terminal-labelled-item', 'text': (
        json.dumps({'status': 'pass', 'summary': 'Earlier item', 'findings': []})
        if earlier_json else 'Earlier terminal-labelled stream item; final assessment follows.')}
    observation['raw_turn_items'] = 2
    observation['turns'][-1]['items'] = [earlier, final]
    observation['turns'][-1]['turn']['items'] = [earlier, final]
    rewrite_observation(case, observation)


@pytest.mark.parametrize('earlier_json', [False, True])
def test_public_native_scope_uses_exact_last_terminal_assessment(blocked_scope, earlier_json):
    case = blocked_scope
    two_finals(case, earlier_json=earlier_json)
    originals = {p: p.read_bytes() for p in case.folder.iterdir() if p.is_file()}
    assert case.store.repair_admission_preflight('run-1', case.command)[
        'authorized_through_iteration'] == 4
    _response, current, recovery = admitted(case)
    assert recovery['state']['roles'][-1]['status'] == 'blocked'
    assert recovery['execution_candidate'] != recovery['state']['candidate']
    assert asyncio.run(delivery_repair_preflight({'spec': current, 'recovery': recovery})) == {
        'state': 'confirmed'}
    assert {p: p.read_bytes() for p in originals} == originals


@pytest.mark.parametrize(
    'later', ['non_json', 'different_json', 'empty', 'nonfinal', 'interrupted'],
)
def test_scope_cannot_search_backwards_for_a_plausible_assessment(blocked_scope, later):
    case = blocked_scope
    observation = read_private(case.folder / 'native-thread-observation.json')
    final = copy.deepcopy(observation['turns'][-1]['items'][-1])
    changed = {**final, 'id': 'last-terminal-item'}
    if later == 'non_json':
        changed['text'] = 'Malformed terminal assessment'
    elif later == 'different_json':
        changed['text'] = json.dumps({'status': 'pass', 'summary': 'Different', 'findings': []})
    elif later == 'empty':
        changed['text'] = ''
    elif later == 'nonfinal':
        final['phase'] = changed['phase'] = 'commentary'
    else:
        observation['turns'][-1]['turn']['status'] = 'interrupted'
    observation['raw_turn_items'] = 2
    observation['turns'][-1]['items'] = [final, changed]
    observation['turns'][-1]['turn']['items'] = [final, changed]
    rewrite_observation(case, observation)
    with pytest.raises(ValueError, match='scope finding'):
        case.store.repair_admission_preflight('run-1', case.command)
