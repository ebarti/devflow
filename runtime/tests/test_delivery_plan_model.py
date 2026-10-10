from copy import deepcopy

import pytest
from test_delivery_github_contract import plan

from devflow_temporal.delivery_plan_model import (
    compact_plan,
    migrate_plan_v1,
    ordered_chunks,
    resolve_plan_issues,
    validate_plan,
)


def v2_plan():
    legacy = plan()
    bindings = {stream["id"]: {"number": stream["issue_number"]}
                for stream in legacy["workstreams"]}
    gates = {chunk["id"]: [] for stream in legacy["workstreams"] for chunk in stream["chunks"]}
    return migrate_plan_v1(legacy, bindings, gates, [])


def test_v2_expected_paths_do_not_replace_legacy_source_authority():
    value = v2_plan()
    value["workstreams"][0]["chunks"][0]["expected_paths"] = ["revised-model.py"]
    assert validate_plan(value, allowed_paths=["model.py"]) == value
    assert [chunk["id"] for chunk in ordered_chunks(value)] == ["model", "client", "endpoint"]
    with pytest.raises(ValueError, match="configured source scope"):
        validate_plan(plan(), allowed_paths=["model.py"])


@pytest.mark.parametrize("injection", ["argv", "env", "ports", "network_domains"])
def test_gate_selection_cannot_grant_recipe_execution_authority(injection):
    value = v2_plan()
    value["workstreams"][0]["chunks"][0]["gates"] = [
        {"stage": "checks", "recipe_id": "unit", "selectors": ["tests/test_model.py"],
         injection: ["arbitrary"]}]
    with pytest.raises(ValueError, match="admitted recipe"):
        validate_plan(value)


@pytest.mark.parametrize("selector", ["../test_escape.py", "/tmp/test_escape.py",
                                      "tests/.git/test_index.py", "--all", "tests//test_a.py"])
def test_v2_selection_rejects_escaping_or_option_operands(selector):
    value = v2_plan()
    value["final_gates"] = [{"stage": "checks", "recipe_id": "unit", "selectors": [selector]}]
    with pytest.raises(ValueError):
        validate_plan(value)


def test_supported_selectors_and_compact_index_keep_detail_on_children():
    value = v2_plan()
    gate = {"stage": "checks", "recipe_id": "unit",
            "selectors": ["tests/test_model.py::test_result"], "reason": "Owned by this chunk"}
    value["workstreams"][0]["chunks"][0]["gates"] = [gate]
    assert validate_plan(value) == value
    compact = compact_plan(value)
    assert set(compact["workstreams"][0]["chunks"][0]) == {"id", "title", "depends_on"}
    assert "Implement" not in str(compact)


def test_explicit_migration_resolves_existing_children_without_mutating_the_old_plan():
    old = plan()
    old["workstreams"][0]["issue_number"] = None
    historical = deepcopy(old)
    bindings = {"api": {"number": 2}, "ui": {"number": 3}}
    gates = {chunk["id"]: [] for stream in old["workstreams"] for chunk in stream["chunks"]}
    migrated = migrate_plan_v1(old, bindings, gates, [])
    assert old == historical
    assert migrated["workstreams"][0]["issue_number"] == 2
    assert "allowed_paths" in old["workstreams"][0]["chunks"][0]
    assert "expected_paths" in migrated["workstreams"][0]["chunks"][0]
    with pytest.raises(ValueError, match="every chunk"):
        migrate_plan_v1(old, bindings, {}, [])
    with pytest.raises(ValueError, match="differs"):
        resolve_plan_issues(plan(), {"api": {"number": 4}, "ui": {"number": 3}})
