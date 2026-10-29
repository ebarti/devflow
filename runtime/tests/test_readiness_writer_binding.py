"""Actual readiness probes retain and authenticate their admitted writer witness."""

import copy

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_prelaunch_capacity import native_role as native_role

from devflow_temporal.delivery_readiness import (
    ReadinessError,
    describe_requirements,
    prepare_and_probe,
)
from devflow_temporal.delivery_readiness_verify import tools
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.delivery_writer_custody import shared


def test_readiness_authenticates_actual_writer_and_refuses_changed_or_missing_binding(native_role):
    case = native_role
    spec, candidate = case.request["spec"], case.request["candidate"]
    requirements = describe_requirements(spec, case.broker.checkout,
                                         accepted_plan=spec["accepted_plan"])
    with shared(spec):
        receipt = prepare_and_probe(case.store, spec, candidate, requirements, "plan")
    assert receipt["state"] == "passed", receipt
    tools(case.store, spec, candidate, requirements, receipt)
    path = receipt["observations"]["tools"]["items"][0]["native_process"]["journal"]
    from pathlib import Path

    path = Path(path)
    original = read_private(path)
    assert original["intent"]["writer_custody"]["run_id"] == spec["run_id"]
    for field in ("run_id", "state_dir", "identity", None):
        changed = copy.deepcopy(original)
        if field is None:
            del changed["intent"]["writer_custody"]
        else:
            changed["intent"]["writer_custody"][field] = "foreign"
        write_private(path, changed)
        with pytest.raises(ReadinessError, match="intent or result changed"):
            tools(case.store, spec, candidate, requirements, receipt)
    write_private(path, original)
