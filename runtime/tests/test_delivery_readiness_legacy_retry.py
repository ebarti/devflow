"""Public automatic retry retains the admitted predecessor's readiness policy."""
import copy
import json
import subprocess

from test_delivery_automatic_retry import stopped
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


def test_legacy_native_automatic_retry_preserves_frozen_readiness(
    native_configuration, monkeypatch
):
    config, request = native_configuration
    source = config.raw['repositories']['fixture']['source_path']
    subprocess.run(['git', '-C', source, 'update-ref', 'refs/heads/main', 'HEAD'], check=True)
    subprocess.run(['git', '-C', source, 'update-ref', 'refs/remotes/origin/main', 'HEAD'],
                   check=True)
    config.raw['repositories']['fixture']['base_ref'] = 'main'
    config.path.write_text(json.dumps(config.raw))
    request = {**request, 'base_ref': 'main', 'accepted_plan': 'Previously accepted plan'}
    original = config.admit(request)
    original.pop('readiness_version')
    original['policy'].pop('readiness')
    original['policy_digest'] = digest(original['policy'])
    store = DeliveryStore(config)
    with monkeypatch.context() as historical:
        historical.setattr(DeliveryConfig, 'admit', lambda *_a, **_kw: copy.deepcopy(original))
        store, request, _ = stopped((store, request), monkeypatch)
    result = retry.retry_once(store)
    assert len(result) == 1
    successor = store.submitted_spec(result[0]['run_id'])
    assert successor['supersedes_run_id'] == request['run_id']
    assert 'readiness_version' not in successor and 'readiness' not in successor['policy']
    assert successor['policy'] == original['policy']
