"""Local clients preserve the typed assessment and do not start the service."""
import io
import json
import sys
import urllib.error
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_control
from devflow_temporal.delivery_client import DeliveryClient
from devflow_temporal.delivery_lifecycle import RecoveryRefused


def test_http_client_preserves_structured_recovery_refusal():
    assessment = {'version': 1, 'eligible': False, 'blockers': [{'code': 'revision_changed'}]}
    client = DeliveryClient(SimpleNamespace(dashboard_url='http://127.0.0.1:9876'))
    client.login = lambda **_kwargs: None
    error = urllib.error.HTTPError('http://127.0.0.1:9876', 409, 'conflict', {},
                                  io.BytesIO(json.dumps({'detail': assessment}).encode()))
    client.opener = SimpleNamespace(open=lambda *_a, **_k: (_ for _ in ()).throw(error))
    with pytest.raises(RecoveryRefused) as refusal:
        client.continue_repair('run-1', {'command_id': 'cmd'})
    assert refusal.value.assessment == assessment


def test_cli_assessment_uses_existing_read_only_client(tmp_path, monkeypatch, capsys):
    config = tmp_path / 'config.json'
    config.write_text('{}')
    request = tmp_path / 'request.json'
    request.write_text(json.dumps({'kind': 'stopped_delivery_resume',
                                   'request': {'expected_revision': 5}}))
    calls = []
    monkeypatch.setattr('devflow_temporal.delivery_native_guard.reject_nested_controller',
                        lambda: None)
    monkeypatch.setattr(delivery_control.DeliveryConfig, 'load',
                        lambda path: SimpleNamespace(path=path))
    monkeypatch.setattr(delivery_control, 'read_only_client', lambda _path: SimpleNamespace(
        assess_transition=lambda run, kind, payload: calls.append((run, kind, payload)) or
        {'version': 1, 'eligible': False, 'blockers': [{'code': 'owner_proof_unknown'}]}))
    monkeypatch.setattr(delivery_control, 'api_client',
                        lambda *_args: pytest.fail('unexpected startup'))
    monkeypatch.setattr(sys, 'argv', [
        'devflow-delivery', '--config', str(config), 'assess-transition',
        '--id', 'run-1', '--request', str(request),
    ])
    delivery_control.main()
    assert json.loads(capsys.readouterr().out)['blockers'][0]['code'] == 'owner_proof_unknown'
    assert calls == [('run-1', 'stopped_delivery_resume', {'expected_revision': 5})]


def test_direct_preflight_reports_the_registry_owner_when_snapshot_changes():
    from devflow_temporal.delivery_api import _recovery_conflict_detail
    from devflow_temporal.delivery_feature_revision_subject import FeatureRegistryObservationError

    assessment = _recovery_conflict_detail(FeatureRegistryObservationError('unavailable'))
    assert assessment['blockers'][0]['owner'] == 'feature_registry'
    assert assessment['blockers'][1]['code'] == 'feature_registry_proof_unknown'
    assert assessment['blockers'][1]['satisfied'] is None
