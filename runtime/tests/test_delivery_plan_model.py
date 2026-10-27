"""Shared v2 plan fixture for public publication and revision workflows."""

from test_delivery_github_contract import plan

from devflow_temporal.delivery_plan_model import (
    migrate_plan_v1,
)


def v2_plan():
    legacy = plan()
    bindings = {stream["id"]: {"number": stream["issue_number"]}
                for stream in legacy["workstreams"]}
    gates = {chunk["id"]: [] for stream in legacy["workstreams"] for chunk in stream["chunks"]}
    return migrate_plan_v1(legacy, bindings, gates, [])
