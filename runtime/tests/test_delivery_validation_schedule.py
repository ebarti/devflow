"""Real commands run once at the required candidate, never a fabricated cache pass."""
from __future__ import annotations

import json
import sys

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_validation_schedule import prepublication, require_deferred_checks


def recipe(ident, argv=None):
    return {"id": ident, "kind": "check", "argv": argv or ["tool", "check"]}


def spec(pre, post):
    return {"validation_schedule_version": 1,
            "policy": {"prepublish_checks": pre, "checks": post}}


def test_dependency_and_history_checks_stay_before_publication():
    dependency = recipe("install", ["corepack", "pnpm", "install", "/store"])
    git = recipe("diff", ["git", "diff", "--check"])
    before = recipe("broad-before")
    after = recipe("broad-after")
    value = spec([dependency, before, git], [dependency, after, git])
    assert prepublication(value) == ([dependency, git], {"broad-before": "broad-after"})
    require_deferred_checks(value, {"deferred_to_checks": {"broad-before": "broad-after"}},
                            {"results": [{**after, "passed": True}]})
    for changed in [{"results": []}, {"results": [{**after, "passed": False}]},
                    {"results": [{**after, "passed": True, "argv": ["different"]}]}]:
        with pytest.raises(ValueError, match="no passing execution"):
            require_deferred_checks(value,
                                    {"deferred_to_checks": {"broad-before": "broad-after"}},
                                    changed)


@pytest.mark.parametrize("field,value", [("argv", ["different"]), ("cwd", "other"),
    ("min_tests", 2), ("required_selectors", ["different.test.ts"]),
    ("network_domains", ["example.invalid"]), ("timeout_seconds", 42)])
def test_changed_validation_inputs_are_not_deduplicated(field, value):
    before, after = recipe("before"), recipe("after")
    after[field] = value
    assert prepublication(spec([before], [after])) == ([before], {})


def test_unique_prerequisite_or_build_only_worker_keeps_requested_order():
    broad = recipe("broad")
    unique = recipe("consumer", ["node", "consume-build-output.js"])
    assert prepublication(spec([broad, unique], [broad])) == ([broad, unique], {})
    value = spec([broad], [broad])
    value["feature_worker"] = {"kind": "build"}
    assert prepublication(value) == ([broad], {})
    value.pop("feature_worker")
    value.pop("validation_schedule_version")
    assert prepublication(value) == ([broad], {})


def test_actual_broker_executes_shared_broad_command_only_after_publication(service):
    original, request = service
    path = original.config.path
    configured = json.loads(path.read_text())
    counter = original.config.state_root / "validation-count.json"
    code = ("from pathlib import Path; import json; "
            f"p=Path({str(counter)!r}); "
            "p.write_text(json.dumps(json.loads(p.read_text())+1 if p.exists() else 1)); "
            "print('checked')")
    before = recipe("before", [sys.executable, "-c", code])
    after = {**before, "id": "after"}
    configured["repositories"]["fixture"].update(prepublish_checks=[before], checks=[after])
    path.write_text(json.dumps(configured))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(request)
    value = store.spec(request["run_id"])
    broker = DeliveryBroker(store, value)
    candidate = broker.prepare()["candidate"]
    prechecked = broker.run_prechecks(0, candidate)
    assert prechecked["state"] == "passed" and prechecked["results"] == []
    assert prechecked["deferred_to_checks"] == {"before": "after"}
    assert not counter.exists()
    checked = broker.run_checks(0, candidate)
    assert checked["state"] == "passed" and json.loads(counter.read_text()) == 1
    require_deferred_checks(value, prechecked, checked)
    # An actual source change must be tested again, not reused from the first pass.
    (broker.checkout / "README.md").write_text("Changed candidate\n")
    _git(broker.checkout, "add", "README.md")
    _git(broker.checkout, "-c", "commit.gpgsign=false", "-c", "user.name=Fixture",
         "-c", "user.email=fixture@example.invalid", "commit", "-qm", "Changed candidate")
    changed = broker.candidate()
    assert changed["id"] != candidate["id"]
    assert broker.run_checks(1, changed)["state"] == "passed"
    assert json.loads(counter.read_text()) == 2
