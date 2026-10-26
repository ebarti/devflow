"""Native allowance preflight authenticates without writing a checkpoint or Git index."""

import hashlib
import json
import sqlite3
from copy import deepcopy

from test_delivery_execution_registry import owner

from devflow_temporal.delivery_feature_execution import _observe_repair_allowance
from devflow_temporal.delivery_feature_handoff import _RegistryView

pytest_plugins = ["test_delivery_execution_registry"]


def test_allowance_observation_preserves_index_budget_and_original_authority(
    registry,
    snapshot,
    installed_runtime,
    tmp_path,
):
    token = owner(registry, snapshot)
    registry.stop(token, "stopped", {})
    tracking = tmp_path / "tracking.sqlite3"
    with sqlite3.connect(tracking) as db:
        db.execute("CREATE TABLE delivery_dashboard_state (run_id TEXT, identity_json TEXT)")
        db.execute(
            "INSERT INTO delivery_dashboard_state VALUES (?,?)",
            (
                token["run_id"],
                json.dumps(
                    {
                        "revision": installed_runtime.baseline["revision"],
                        "dirty": False,
                        "local_digest": None,
                    }
                ),
            ),
        )

    class Store:
        def _connect(self):
            return sqlite3.connect(tracking)

    original = {
        "policy": {
            "native_identity": {
                "runtime_payload_sha256": installed_runtime.baseline["runtime_payload_sha256"]
            }
        }
    }
    frozen = deepcopy(original)
    budget = registry.budget(token["issue_id"])
    database_hash = hashlib.sha256(registry.path.read_bytes()).hexdigest()
    index = installed_runtime.root / ".git" / "index"
    index_bytes = index.read_bytes()
    with registry.connect() as db:
        _observe_repair_allowance(Store(), _RegistryView(db), token, original)
    assert hashlib.sha256(registry.path.read_bytes()).hexdigest() == database_hash
    assert index.read_bytes() == index_bytes
    assert original == frozen
    assert registry.budget(token["issue_id"]) == budget
    admitted = registry.renew_repair_allowance(token, installed_runtime.baseline)
    assert registry.renew_repair_allowance(token, installed_runtime.baseline) == admitted
    assert admitted["allowance"]["sequence"] == 0
    assert admitted["used"] == budget["used"]
