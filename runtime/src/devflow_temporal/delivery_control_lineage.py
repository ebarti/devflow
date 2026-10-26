"""Server-admitted operational timer controls, separate from frozen authorization.

Only successor admission mints this carrier. Its hashes bind retained evidence;
SQLite admission and the accepted control event supply authority, not the hashes.
"""
from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime

from .contracts import digest

FIELD = "delivery_control_lineage"


def _control(value):
    if (not isinstance(value, dict) or set(value) != {
            "disabled", "command_id", "reason", "recorded_at", "protocol_revision"}
            or value["disabled"] is not True
            or type(value["protocol_revision"]) is not int
            or value["protocol_revision"] < 1
            or not isinstance(value["command_id"], str)
            or not value["command_id"].strip() or len(value["command_id"]) > 128
            or not isinstance(value["reason"], str)
            or not value["reason"].strip() or len(value["reason"]) > 4000
            or not isinstance(value["recorded_at"], str)):
        raise ValueError("inherited delivery-limit control is malformed")
    try:
        recorded = datetime.fromisoformat(value["recorded_at"])
    except ValueError as exc:
        raise ValueError("inherited delivery-limit timestamp is malformed") from exc
    if recorded.tzinfo is None:
        raise ValueError("inherited delivery-limit timestamp has no timezone")
    return value


def control_from_spec(spec):
    """Validate a previously server-admitted carrier before timer evaluation."""
    value = spec.get(FIELD)
    if value is None:
        return None
    if (not isinstance(value, dict) or set(value) != {
            "version", "run_id", "request_digest", "issue_url", "delivery_admitted_at",
            "predecessor", "origin", "control"} or value.get("version") != 1
            or any(value.get(key) != spec.get(key) for key in (
                "run_id", "request_digest", "issue_url", "delivery_admitted_at"))):
        raise ValueError("delivery-limit lineage changed its admitted execution")
    control = _control(value["control"])
    predecessor, origin = value["predecessor"], value["origin"]
    if (not isinstance(predecessor, dict) or set(predecessor) != {
            "run_id", "request_digest", "control_sha256", "lineage_sha256"}
            or not isinstance(origin, dict) or set(origin) != {
                "run_id", "request_digest", "event_sha256"}
            or not all(isinstance(item, str) and item for item in (
                predecessor["run_id"], predecessor["request_digest"],
                predecessor["lineage_sha256"], origin["run_id"],
                origin["request_digest"], origin["event_sha256"]))
            or predecessor["control_sha256"] != digest(control)
            or predecessor["run_id"] == spec["run_id"]):
        raise ValueError("delivery-limit lineage has invalid predecessor evidence")
    return deepcopy(control)


def _event(db, run_id, control):
    rows = db.execute(
        "SELECT payload_json FROM delivery_events WHERE run_id=? "
        "AND type='delivery_limits_disabled' AND json_extract(payload_json,'$.key')=? "
        "LIMIT 2", (run_id, "disable-limits:" + control["command_id"]),
    ).fetchall()
    if len(rows) != 1:
        return None
    value = json.loads(rows[0][0])
    return value if value.get("control") == control else None


def inherit_delivery_control(store, db, spec, predecessor_row, *, closed=None):
    """Mint a control carrier inside an already authenticated successor transaction.

    The caller owns closure, owner and continuation eligibility checks. Repeat the
    exact local-row check here, then authenticate the control against its original
    accepted event. Caller-supplied dictionaries never become an admission grant.
    """
    if FIELD in spec:
        raise ValueError("delivery-limit lineage must be minted by successor admission")
    predecessor_row = dict(predecessor_row)
    current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                         (predecessor_row["run_id"],)).fetchone()
    if current is None or dict(current) != predecessor_row:
        raise ValueError("delivery-limit predecessor changed before admission")
    previous = json.loads(predecessor_row["request_json"])
    checks = json.loads(predecessor_row["checks_json"] or "{}")
    control = checks.get("delivery_limits_control")
    inherited = control_from_spec(previous)
    observed = (closed or {}).get("result", {}).get("checks", {}).get(
        "delivery_limits_control")
    if control is None:
        if inherited is not None or observed is not None:
            raise ValueError("delivery-limit control is absent from current predecessor")
        return
    _control(control)
    if (store.state.issue_resource(spec["issue_url"])
            != store.state.issue_resource(previous["issue_url"])
            or spec.get("delivery_admitted_at") != previous.get("delivery_admitted_at")):
        raise ValueError("delivery-limit control changed its original issue or clock")
    if closed is not None and (
            closed.get("request_digest") != predecessor_row["request_digest"]
            or closed.get("result", {}).get("run_id") != previous["run_id"]
            or observed != control):
        raise ValueError("closed predecessor disagrees with accepted delivery-limit control")
    event = _event(db, previous["run_id"], control)
    if event is not None:
        origin = {"run_id": previous["run_id"],
                  "request_digest": predecessor_row["request_digest"],
                  "event_sha256": digest(event)}
    else:
        if inherited != control:
            raise ValueError("delivery-limit control has no recorded accepted event")
        origin = deepcopy(previous[FIELD]["origin"])
        root = db.execute("SELECT request_digest,request_json FROM delivery_runs WHERE run_id=?",
                          (origin["run_id"],)).fetchone()
        event = _event(db, origin["run_id"], control)
        if (root is None or root["request_digest"] != origin["request_digest"]
                or event is None or digest(event) != origin["event_sha256"]
                or store.state.issue_resource(json.loads(root["request_json"])["issue_url"])
                != store.state.issue_resource(previous["issue_url"])):
            raise ValueError("inherited delivery-limit event is no longer authentic")
    spec[FIELD] = {
        "version": 1, "run_id": spec["run_id"], "request_digest": spec["request_digest"],
        "issue_url": spec["issue_url"], "delivery_admitted_at": spec.get("delivery_admitted_at"),
        "predecessor": {"run_id": previous["run_id"],
                        "request_digest": predecessor_row["request_digest"],
                        "control_sha256": digest(control),
                        "lineage_sha256": digest(previous.get(FIELD))},
        "origin": origin, "control": deepcopy(control),
    }
    control_from_spec(spec)
