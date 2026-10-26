"""Literal workflow formats; semantic coverage never comes from names or prose."""
from __future__ import annotations

import copy

import pytest
import yaml

from devflow_temporal import delivery_ci_inventory_contract as contract
from devflow_temporal.contracts import digest


@pytest.mark.parametrize("value,expected", [
    ("${{ github.event_name != 'pull_request' || github.event.pull_request.head.x == 'x' }}",
     True),
    ("github.ref == 'refs/heads/main' && vars.UNKNOWN == 'true'", False),
    ("inputs.deploy || (runner.os == 'Linux' && runner.environment == 'github-hosted')", True),
    ("success() && true", True),
    ("false || false", False),
])
def test_authored_literal_conditions_are_fully_determined(value, expected):
    assert contract.condition(value, {'github.event_name': 'workflow_dispatch',
        'github.ref': 'refs/heads/fixture', 'inputs.deploy': False, 'runner.os': 'Linux',
        'runner.environment': 'github-hosted', 'prior_steps_success': True}) is expected


@pytest.mark.parametrize("value", ["vars.UNKNOWN", "contains('a','b')", "a ==", "(true", "1"])
def test_unknown_or_unsupported_conditions_never_default_to_success(value):
    with pytest.raises(ValueError):
        contract.condition(value, {})


@pytest.mark.parametrize("source", [
    b'on: {workflow_dispatch: null}\njobs: {}\njobs: {}\n',
    b'field: &same {value: true}\nother: *same\n',
])
def test_duplicate_or_alias_workflow_sources_are_refused(source):
    with pytest.raises(ValueError):
        contract.document(source)


@pytest.fixture
def workflow():
    return {'name': 'Neutral owned CI', 'on': {'workflow_dispatch': None}, 'jobs': {
        'python': {'name': 'Python ${{ matrix.python-version }}', 'runs-on': 'ubuntu-latest',
            'strategy': {'matrix': {'python-version': ['3.11', '3.12', '3.13']}}, 'steps': [
                {'name': 'Checkout', 'uses': 'actions/checkout@' + 'a' * 40},
                {'name': 'Literal check', 'if': "${{ runner.os == 'Linux' }}",
                 'run': 'python${{ matrix.python-version }} -m pytest tests'}]},
        'excluded': {'name': 'Excluded publication', 'runs-on': 'ubuntu-latest',
            'if': "github.ref == 'refs/heads/main' && inputs.deploy", 'steps': [
                {'name': 'Excluded opaque input', 'uses': 'example/action@' + 'b' * 40,
                 'with': {'token': '${{ secrets.OPAQUE }}'}}]}}}


def test_complete_matrix_and_false_publication_condition_preserve_source(workflow):
    raw = yaml.safe_dump(workflow).encode()
    plan = contract.workflow_plan(raw, repository='example/fixture', branch='owned/fixture',
                                  inputs={'deploy': False})
    assert [x['name'] for x in plan] == [
        'Excluded publication', 'Python 3.11', 'Python 3.12', 'Python 3.13']
    assert plan[0]['enabled'] is False
    assert plan[0]['steps'][0]['source']['with']['token'] == '${{ secrets.OPAQUE }}'
    assert [x['steps'][1]['source']['run'] for x in plan[1:]] == [
        'python3.11 -m pytest tests', 'python3.12 -m pytest tests', 'python3.13 -m pytest tests']
    assert all(x['steps'][1]['enabled'] is True for x in plan[1:])


@pytest.mark.parametrize('change', ['matrix', 'runner', 'reusable', 'duplicate', 'expansion'])
def test_unsupported_source_selection_never_produces_an_approved_plan(workflow, change):
    value = copy.deepcopy(workflow)
    python = value['jobs']['python']
    if change == 'matrix':
        python['strategy']['matrix']['include'] = [{'python-version': '3.14'}]
    elif change == 'runner':
        python['runs-on'] = 'self-hosted'
    elif change == 'reusable':
        python['uses'] = 'example/workflow@' + 'a' * 40
    elif change == 'duplicate':
        python['steps'][1]['name'] = 'Checkout'
    else:
        python['steps'][1]['run'] = '${{ unknown.expression }}'
    with pytest.raises(ValueError):
        contract.workflow_plan(yaml.safe_dump(value).encode(), repository='example/fixture',
                               branch='owned/fixture', inputs={'deploy': False})


def test_profile_is_a_fixed_authority_pin_never_a_caller_result(monkeypatch):
    neutral = {'schema': contract.SCHEMA, 'selection': [1, 2, 3, 4, 5]}
    monkeypatch.setattr(contract, 'APPROVED_PROFILE_SHA256', digest(neutral))
    assert contract.approved_profile(neutral) == neutral
    with pytest.raises(ValueError):
        contract.approved_profile({**neutral, 'CI_success': True})
