#!/usr/bin/env python3
"""Record supplied workflow facts in SQLite. This script does not execute work."""

import argparse
from contextlib import closing
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import uuid


FIELDS = {
    "work": "id title repository issue branch commit status stage blocker started_at ended_at details",
    "run": "id work_id agent role model effort status started_at ended_at duration_seconds source_ref recorded_at details",
    "result": "id work_id run_id kind status commit evidence_ref summary recorded_at details",
    "finding": "id work_id summary severity status commit evidence_ref fix_ref thread_ref recorded_at details",
    "usage": "id work_id run_id agent model input_tokens cached_input_tokens cache_write_tokens output_tokens reasoning_output_tokens estimated_cost_usd estimated_credits source_ref recorded_at allocations details",
}
TABLES = {"work": "works", "run": "runs", "result": "results", "finding": "findings", "usage": "usage"}
NUMBERS = {"duration_seconds", "estimated_cost_usd", "estimated_credits"}
TOKENS = {"input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens", "reasoning_output_tokens"}
APP_ID = 0x44564632


def now():
    return datetime.now(timezone.utc).isoformat()


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


def instant(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return result


def connect(path):
    path = Path(path).expanduser()
    previous_umask = os.umask(0o077)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(path, timeout=30)
    finally:
        os.umask(previous_umask)
    db.row_factory = sqlite3.Row
    try:
        db.execute("PRAGMA foreign_keys=ON")
        db.execute("BEGIN IMMEDIATE")
        version = db.execute("PRAGMA user_version").fetchone()[0]
        if version == 0 and not db.execute("SELECT 1 FROM sqlite_master WHERE type='table'").fetchone():
            # Statements are deliberately split by complete_statement, not executescript,
            # which would commit the transaction before the schema is created.
            statement = ""
            for line in Path(__file__).with_name("schema.sql").read_text().splitlines(True):
                statement += line
                if sqlite3.complete_statement(statement):
                    db.execute(statement)
                    statement = ""
            db.execute(f"PRAGMA application_id={APP_ID}")
            db.execute("PRAGMA user_version=2")
        elif version != 2 or db.execute("PRAGMA application_id").fetchone()[0] != APP_ID:
            raise ValueError("not a supported workflow.sqlite3 database; use import-legacy for version-1 state.sqlite3")
        db.commit()
        return db
    except BaseException:
        db.close()
        raise


def validate(kind, values):
    unknown = set(values) - set(FIELDS[kind].split())
    if unknown:
        raise ValueError("unknown fields: " + ", ".join(sorted(unknown)))
    for key, value in values.items():
        if value is None:
            continue
        if key in NUMBERS | TOKENS:
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
                raise ValueError(f"{key} must be a finite nonnegative number")
            if key in TOKENS and not isinstance(value, int):
                raise ValueError(f"{key} must be an integer")
        elif key == "details":
            encode(value)
        elif key == "allocations":
            if not isinstance(value, list):
                raise ValueError("allocations must be a list")
        elif not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} must be nonempty text or null")
        if key.endswith("_at"):
            instant(value)
    if values.get("started_at") and values.get("ended_at"):
        elapsed = (instant(values["ended_at"]) - instant(values["started_at"])).total_seconds()
        if elapsed < 0:
            raise ValueError("ended_at precedes started_at")
        if kind == "run" and values.get("duration_seconds") is None:
            values["duration_seconds"] = elapsed


def row(db, table, identity):
    found = db.execute(f'SELECT * FROM {table} WHERE id=?', (identity,)).fetchone()
    return dict(found) if found else None


def insert(db, table, values):
    fields = ",".join(f'"{key}"' for key in values)
    placeholders = ",".join("?" for _ in values)
    db.execute(f"INSERT INTO {table} ({fields}) VALUES ({placeholders})", tuple(values.values()))


def history(db, identity, work_id, entity, entity_id, action, details, **extra):
    insert(db, "history", dict(id=identity, work_id=work_id, entity=entity,
           entity_id=entity_id, action=action, recorded_at=now(), details=encode(details), **extra))


def record(db, kind, supplied):
    values = dict(supplied)
    validate(kind, values)
    required = {"id"}
    if kind != "usage":
        required.add("title" if kind == "work" else "work_id")
    if kind == "result":
        required.update(("kind", "status"))
    if kind == "finding":
        required.add("summary")
    if any(not values.get(key) for key in required):
        raise ValueError("required fields: " + ", ".join(sorted(required)))
    allocations = None
    if kind == "usage":
        work_id = values.pop("work_id", None)
        allocations = values.pop("allocations", None)
        if work_id is not None and allocations is not None:
            raise ValueError("supply work_id or allocations, not both")
        allocations = [{"work_id": work_id, "weight": 1.0}] if work_id else (allocations or [])
        seen = set()
        for item in allocations:
            if not isinstance(item, dict) or set(item) != {"work_id", "weight"}:
                raise ValueError("each allocation needs work_id and weight")
            weight = item["weight"]
            if not isinstance(item["work_id"], str) or not item["work_id"] or item["work_id"] in seen:
                raise ValueError("allocations require distinct work IDs")
            if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not math.isfinite(weight) or not 0 <= weight <= 1:
                raise ValueError("allocation weight must be between zero and one")
            seen.add(item["work_id"])
        if sum(item["weight"] for item in allocations) > 1 + 1e-12:
            raise ValueError("usage allocation exceeds one")
        allocations = sorted(allocations, key=lambda item: item["work_id"])
    if "details" in values:
        values["details"] = encode(values["details"]) if values["details"] is not None else None
    prior = row(db, TABLES[kind], values["id"])
    if prior:
        # The ordinary columns are canonical; do not store a second full payload
        # for replay. Missing optional facts normalize to NULL, except timestamps
        # generated at insertion time, which a retry need not supply.
        compare = set(FIELDS[kind].split()) - {"allocations"}
        if kind == "usage":
            compare.discard("work_id")
        if values.get("recorded_at") is None:
            compare.discard("recorded_at")
        same = all(prior.get(key) == values.get(key) for key in compare)
        if allocations is not None:
            saved = [dict(item) for item in db.execute("SELECT work_id,weight FROM usage_allocations WHERE usage_id=? ORDER BY work_id", (values["id"],))]
            same = same and saved == allocations
        if not same:
            raise ValueError("record ID already exists with different facts")
        return {"replayed": True, "record": prior}
    timestamp = now()
    if kind == "work":
        values.update(created_at=timestamp, updated_at=timestamp)
    else:
        values["recorded_at"] = values.get("recorded_at") or timestamp
        if kind == "finding":
            values["updated_at"] = timestamp
    insert(db, TABLES[kind], values)
    if allocations is not None:
        for allocation in allocations:
            insert(db, "usage_allocations", dict(usage_id=values["id"], **allocation))
    return {"replayed": False, "record": row(db, TABLES[kind], values["id"])}


def update(db, kind, supplied, event_id):
    values = dict(supplied)
    validate(kind, values)
    identity = values.pop("id", None)
    if not identity or not values:
        raise ValueError("update requires id and at least one changed field")
    if "work_id" in values or "recorded_at" in values:
        raise ValueError("work_id and recorded_at cannot be updated")
    key = "update:" + (event_id or uuid.uuid4().hex)
    prior = row(db, "history", key)
    if prior:
        expected = json.loads(prior["details"])["after"]
        if prior["entity"] != kind or prior["entity_id"] != identity or expected != values:
            raise ValueError("event ID already exists with different changes")
        return {"replayed": True, "record": row(db, TABLES[kind], identity)}
    old = row(db, TABLES[kind], identity)
    if old is None:
        raise ValueError("unknown " + kind + ": " + identity)
    merged = {key: old.get(key) for key in FIELDS[kind].split() if key in old}
    merged.update(values)
    if kind == "run" and {"started_at", "ended_at"}.intersection(values) and "duration_seconds" not in values:
        merged["duration_seconds"] = None
    validate(kind, merged)
    changes = {"before": {key: old.get(key) for key in values}, "after": dict(values)}
    if kind == "run" and merged.get("duration_seconds") != old.get("duration_seconds"):
        if "duration_seconds" not in values:
            changes["derived_duration_seconds"] = {"before": old.get("duration_seconds"), "after": merged.get("duration_seconds")}
        values["duration_seconds"] = merged.get("duration_seconds")
    if "details" in values:
        values["details"] = encode(values["details"]) if values["details"] is not None else None
    if all(old.get(key) == value for key, value in values.items()):
        if event_id:
            history(db, key, identity if kind == "work" else old["work_id"], kind, identity, "no_change", changes)
        return {"changed": False, "record": old}
    if kind != "run":
        values["updated_at"] = now()
    assignment = ",".join(f'"{key}"=?' for key in values)
    db.execute(f"UPDATE {TABLES[kind]} SET {assignment} WHERE id=?", (*values.values(), identity))
    history(db, key, identity if kind == "work" else old["work_id"], kind, identity, "update", changes)
    return {"replayed": False, "record": row(db, TABLES[kind], identity)}


def metrics(db, work_id=None):
    where, args = (" WHERE work_id=?", (work_id,)) if work_id else ("", ())
    def counts(table, field, clause=where):
        return [dict(item) for item in db.execute(f'SELECT "{field}", COUNT(*) AS count FROM {table}{clause} GROUP BY "{field}"', args)]
    report = {"work_status": counts("works", "status", " WHERE id=?" if work_id else ""),
              "work_stage": counts("works", "stage", " WHERE id=?" if work_id else ""),
              "results": [dict(item) for item in db.execute("SELECT kind,status,COUNT(*) AS count FROM results" + where + " GROUP BY kind,status", args)],
              "findings": [dict(item) for item in db.execute("SELECT severity,status,COUNT(*) AS count FROM findings" + where + " GROUP BY severity,status", args)],
              "runs": dict(db.execute("SELECT COUNT(*) AS count,SUM(duration_seconds) AS known_duration_seconds, COUNT(*)-COUNT(duration_seconds) AS missing_duration_count FROM runs" + where, args).fetchone())}
    columns = sorted(TOKENS | {"estimated_cost_usd", "estimated_credits"})
    source = "usage u JOIN usage_allocations a ON a.usage_id=u.id WHERE a.work_id=? AND a.weight>0" if work_id else "usage u"
    weight = "a.weight" if work_id else "1.0"
    sums = ",".join(f'SUM(u.{key}*{weight}) AS {key},COUNT(*)-COUNT(u.{key}) AS missing_{key}_count' for key in columns)
    report["usage"] = dict(db.execute(f"SELECT COUNT(*) AS observations,{sums} FROM {source}", args).fetchone())
    report["usage"]["basis"] = "allocated share" if work_id else "distinct observations, including unallocated usage"
    report["limitations"] = ["Missing observations cannot be counted; missing-field counts cover recorded observations only.",
                              "Run duration is summed agent time, not elapsed wall time; unfinished runs have no inferred duration.",
                              "Input/output totals already include cached/reasoning subsets. Cost and credits are supplied estimates."]
    return report


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    default = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state"))) / "devflow/workflow.sqlite3"
    root.add_argument("--db", default=str(default))
    commands = root.add_subparsers(dest="command", required=True)
    def fields(command, kind, updating=False):
        command.add_argument("--file", help="JSON object; flags override supplied fields")
        if updating:
            command.add_argument("--event-id")
        for field in FIELDS[kind].split():
            command.add_argument("--" + field.replace("_", "-"),
                                 type=int if field in TOKENS else float if field in NUMBERS else json.loads if field in {"details", "allocations"} else str)
    work = commands.add_parser("work").add_subparsers(dest="action", required=True)
    fields(work.add_parser("create"), "work")
    fields(work.add_parser("update"), "work", True)
    listing = work.add_parser("list")
    listing.add_argument("--status")
    listing.add_argument("--repository")
    work.add_parser("show").add_argument("--id", required=True)
    records = commands.add_parser("record").add_subparsers(dest="kind", required=True)
    for kind in ("run", "result", "finding", "usage"):
        fields(records.add_parser(kind), kind)
    finding = commands.add_parser("finding").add_subparsers(dest="action", required=True)
    fields(finding.add_parser("update"), "finding", True)
    run = commands.add_parser("run").add_subparsers(dest="action", required=True)
    fields(run.add_parser("update"), "run", True)
    commands.add_parser("metrics").add_argument("--work-id")
    commands.add_parser("import-legacy").add_argument("source")
    return root


def main():
    args = parser().parse_args()
    try:
        values = {}
        if getattr(args, "file", None):
            values = json.loads(Path(args.file).read_text())
            if not isinstance(values, dict):
                raise ValueError("--file must contain one JSON object")
        kind = getattr(args, "kind", args.command)
        if kind in FIELDS:
            values.update({key: getattr(args, key) for key in FIELDS[kind].split() if getattr(args, key, None) is not None})
        with closing(connect(args.db)) as db, db:
            db.execute("BEGIN IMMEDIATE")
            if args.command == "metrics":
                result = metrics(db, args.work_id)
            elif args.command == "import-legacy":
                from legacy import import_legacy
                result = import_legacy(db, args.source, args.db)
            elif args.command == "work" and args.action == "list":
                filters = {key: getattr(args, key) for key in ("status", "repository") if getattr(args, key)}
                where = " WHERE " + " AND ".join(key + "=?" for key in filters) if filters else ""
                result = [dict(item) for item in db.execute("SELECT * FROM works" + where + " ORDER BY created_at,id", tuple(filters.values()))]
            elif args.command == "work" and args.action == "show":
                result = row(db, "works", args.id)
                if result is None:
                    raise ValueError("unknown work: " + args.id)
                for table in ("runs", "results", "findings", "history"):
                    result[table] = [dict(item) for item in db.execute(f"SELECT * FROM {table} WHERE work_id=? ORDER BY recorded_at,id", (args.id,))]
                result["usage"] = [dict(item) for item in db.execute("SELECT u.*,a.weight FROM usage u JOIN usage_allocations a ON a.usage_id=u.id WHERE a.work_id=? ORDER BY u.recorded_at,u.id", (args.id,))]
            elif args.command in {"work", "finding", "run"} and args.action == "update":
                result = update(db, kind, values, args.event_id)
            else:
                result = record(db, kind, values)
        print(encode(result))
    except (ValueError, OSError, sqlite3.Error) as exc:
        print(encode({"error": str(exc)}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
