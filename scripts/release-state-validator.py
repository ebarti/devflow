"""Classify saved release custody; never collect or change runtime state.

The caller pins baseline.json's SHA256 from the frozen release approval.  This
uses capture-backups.py's exact raw-row canonicalization and fingerprint.  Only
manifest-named immutable rollback files are opened.  PASS covers the supplied
saved custody, not freshness, CI, readiness, release authority or product gates.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import plistlib
import re
import sqlite3
import time
import uuid
from collections import Counter
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

OWNERS = {str(i) for i in range(6)}


class Refusal(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise Refusal(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def producer_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def digest_rows(rows):
    values = sorted(rows.elements())
    return {"rows": len(values), "sha256": hashlib.sha256(canonical(values).encode()).hexdigest()}


def raw_row(row):
    return [{"blob": value.hex()} if isinstance(value, bytes) else value for value in row]


def stamp(value):
    require(isinstance(value, str), "missing or non-string timestamp")
    parsed = datetime.fromisoformat(value)
    require(parsed.tzinfo is not None and parsed.utcoffset().total_seconds() == 0
            and value == parsed.isoformat(), "timestamp differs from exact UTC producer format")
    return parsed


def advance(old, new, field):
    require(stamp(new[field]) > stamp(old[field]), f"{field} did not advance")


def exact_except(old, new, fields, context):
    require(set(old) == set(new), f"{context}: changed field set")
    require({k: v for k, v in old.items() if k not in fields}
            == {k: v for k, v in new.items() if k not in fields},
            f"{context}: unapproved field change")


def pairs(old, new, keys, context):
    def index(rows):
        result = {}
        for row in rows:
            key = tuple(row[k] for k in keys)
            require(key not in result, f"{context}: duplicate identity")
            result[key] = row
        return result
    before, after = index(old), index(new)
    require(before.keys() == after.keys(), f"{context}: added/deleted identity")
    return [(before[key], after[key]) for key in before]


def clock_rows(old, new, keys, interval, context):
    for before, after in pairs(old, new, keys, context):
        exact_except(before, after, {"attempts", "next_check_at", "updated_at"}, context)
        require(type(before["attempts"]) is int and type(after["attempts"]) is int
                and after["attempts"] > before["attempts"], f"{context}: attempt did not advance")
        for row in (before, after):
            delay = (stamp(row["next_check_at"]) - stamp(row["updated_at"])).total_seconds()
            require(delay in interval, f"{context}: unsupported retry interval")
        require((stamp(before["next_check_at"]) - stamp(before["updated_at"]))
                == (stamp(after["next_check_at"]) - stamp(after["updated_at"])),
                f"{context}: retry interval changed")
        advance(before, after, "updated_at")
        advance(before, after, "next_check_at")
        delay = (stamp(before["next_check_at"]) - stamp(before["updated_at"])).total_seconds()
        require((stamp(after["updated_at"]) - stamp(before["updated_at"])).total_seconds()
                >= (after["attempts"] - before["attempts"]) * delay,
                f"{context}: retry counter exceeds elapsed authenticated backoff")


def pr_payload(before, after, observations, context):
    """A same-observation refresh changes only the exact observed PR clock."""
    exact_except(before, after, {"pull_requests"}, context)
    changed = False
    require(isinstance(before.get("pull_requests"), list)
            and len(before["pull_requests"]) == len(after["pull_requests"]),
            f"{context}: PR set changed")
    for old, new in zip(before["pull_requests"], after["pull_requests"], strict=True):
        exact_except(old, new, {"checked_at"}, context)
        if old != new:
            advance(old, new, "checked_at")
            observation = observations.get(new["url"])
            require(observation is not None and new["checked_at"] == observation["checked_at"],
                    f"{context}: PR clock is not its authoritative observation")
            changed = True
    return changed


def outbox_identity(payload):
    fields = ("issue", "run_id", "source", "version", "status", "binding", "legacy_tracking")
    return hashlib.sha256(producer_json({k: payload[k] for k in fields}).encode()).hexdigest()


def wire(data, allowed):
    """The finite Temporal 1.32 persistence messages used by idle maintenance."""
    result, offset = {}, 0

    def varint():
        nonlocal offset
        value = 0
        for shift in range(0, 70, 7):
            require(offset < len(data), "truncated protobuf varint")
            byte = data[offset]
            offset += 1
            require(shift < 63 or byte <= 1, "overlong protobuf varint")
            value |= (byte & 127) << shift
            if byte < 128:
                require(shift == 0 or byte != 0, "noncanonical protobuf varint")
                return value
        raise Refusal("overlong protobuf varint")

    while offset < len(data):
        tag = varint()
        field, kind = tag >> 3, tag & 7
        require(field in allowed and kind in (0, 2), f"unsupported protobuf field/wire {field}/{kind}")
        value = varint()
        if kind == 2:
            require(offset + value <= len(data), "truncated protobuf bytes")
            value, offset = data[offset:offset + value], offset + value
        result.setdefault(field, []).append((kind, value))
    return result


def scalar(message, field, kind, default=None):
    if field not in message:
        require(default is not None, f"missing protobuf field {field}")
        return default
    values = message[field]
    require(len(values) == 1 and values[0][0] == kind, f"invalid protobuf scalar {field}")
    return values[0][1]


def proto_time(data):
    fields = wire(data, {1, 2})
    seconds, nanos = scalar(fields, 1, 0, 0), scalar(fields, 2, 0, 0)
    require(seconds < 2**63 and 0 <= nanos < 1_000_000_000, "invalid protobuf timestamp")
    return seconds, nanos


def proto_map(message, field, value_kind, decode=lambda value: value):
    result = {}
    for kind, data in message.get(field, []):
        require(kind == 2, "invalid protobuf map wire")
        entry = wire(data, {1, 2})
        key = scalar(entry, 1, 2)
        require(key not in result, "duplicate protobuf map key")
        result[key] = decode(scalar(entry, 2, value_kind))
    return result


def blob(value):
    require(isinstance(value, dict) and set(value) == {"blob"}
            and isinstance(value["blob"], str), "invalid raw BLOB custody")
    data = bytes.fromhex(value["blob"])
    require(data.hex() == value["blob"], "noncanonical raw BLOB")
    return data


def message(row, allowed):
    require(row["data_encoding"] == "Proto3", "unsupported Temporal data encoding")
    return wire(blob(row["data"]), allowed)


def loopback(value):
    text = value.decode("ascii")
    match = re.fullmatch(r"127\.0\.0\.1:([0-9]+)", text)
    require(match is not None and 0 < int(match[1]) < 65536, "Temporal address is not valid loopback")
    return text


def owner_address(value):
    text = value.decode("ascii")
    match = re.fullmatch(r"(127\.0\.0\.1:[0-9]+)-1-([0-9a-f-]{36})", text)
    require(match is not None, "invalid Temporal shard owner")
    loopback(match[1].encode())
    require(str(uuid.UUID(match[2])) == match[2], "invalid Temporal shard owner UUID")


def task_key(data):
    fields = wire(data, {1, 2})
    task_id = scalar(fields, 2, 0, 0)
    require(task_id < 2**63, "negative or out-of-range signed TaskKey ID")
    return proto_time(scalar(fields, 1, 2, b"")), task_id


def shard_queues(fields):
    def decode(data):
        queue = wire(data, {2})
        return task_key(scalar(queue, 2, 2))
    result = {}
    for kind, data in fields.get(17, []):
        require(kind == 2, "invalid shard queue map")
        entry = wire(data, {1, 2})
        key = scalar(entry, 1, 0)
        require(key not in result, "duplicate shard category")
        result[key] = decode(scalar(entry, 2, 2))
    require(set(result) == {1, 2, 4, 7}, "unsupported shard category set")
    return result


def queue_info(row):
    fields = message(row, {1, 2, 3, 4, 5, 6, 7, 9})
    namespace = scalar(fields, 1, 2).decode("ascii")
    require(str(uuid.UUID(namespace)) == namespace, "invalid queue namespace")
    name, tasktype, kind = scalar(fields, 2, 2), scalar(fields, 3, 0), scalar(fields, 4, 0)
    require(blob(row["task_queue_id"]) == uuid.UUID(namespace).bytes + name + bytes([tasktype]),
            "queue SQL identity does not match namespace/name/type")
    ack = scalar(fields, 5, 0, 0)
    require(ack < 2**63, "negative or out-of-range signed parent ACK")
    subqueues = {}
    for wiretype, data in fields.get(9, []):
        require(wiretype == 2, "invalid subqueue wire")
        subqueue = wire(data, {1, 2})
        key = scalar(subqueue, 1, 2)
        require(wire(key, {1}) == {1: [(0, 3)]} and key not in subqueues,
                "unknown or duplicate subqueue key")
        subqueues[key] = scalar(subqueue, 2, 0, 0)
        require(subqueues[key] < 2**63, "negative or out-of-range signed subqueue ACK")
    require(len(subqueues) == 1 and next(iter(subqueues.values())) == ack,
            "parent/subqueue ACKs do not agree")
    return fields, namespace, name.decode("utf-8"), tasktype, kind, ack


def temporal_rows(table, before, after, phase, startup=None, namespaces=None):
    if table == "shards":
        for old, new in pairs(before, after, ("shard_id",), table):
            if old == new:
                continue
            exact_except(old, new, {"range_id", "data"}, table)
            a, b = (message(row, {1, 2, 3, 7, 17}) for row in (old, new))
            for row, fields in ((old, a), (new, b)):
                require(scalar(fields, 1, 0) == row["shard_id"]
                        and scalar(fields, 2, 0) == row["range_id"], "shard SQL/proto lease mismatch")
            require(proto_time(scalar(b, 7, 2)) >= proto_time(scalar(a, 7, 2)), "shard update clock decreased")
            if phase == "preinstall":
                require(old["range_id"] == new["range_id"] and scalar(a, 3, 2) == scalar(b, 3, 2),
                        "preinstall shard owner/lease changed")
            else:
                require(new["range_id"] == old["range_id"] + 1 and scalar(a, 3, 2) != scalar(b, 3, 2),
                        "restart shard owner/lease is not one new lease")
                owner_address(scalar(b, 3, 2))
            qa, qb = shard_queues(a), shard_queues(b)
            require(qa[1] == qb[1], "transfer queue watermark changed")
            for category in (2, 4, 7):
                require(qb[category] >= qa[category], "shard watermark decreased")
                require(qb[category][0] == qa[category][0] if category != 2 else
                        qb[category][1] == qa[category][1], "unsupported watermark field change")
                if phase == "preinstall" and category != 2:
                    require(qb[category] == qa[category], "preinstall visibility/archival state changed")
    elif table == "task_queues":
        old_index = {(r["range_hash"], canonical(r["task_queue_id"])): r for r in before}
        new_index = {(r["range_hash"], canonical(r["task_queue_id"])): r for r in after}
        require(len(old_index) == len(before) and len(new_index) == len(after)
                and old_index.keys() <= new_index.keys(), "original queue identity removed/duplicated")
        original_hosts = set()
        for row in before:
            _, _, name, _, kind, _ = queue_info(row)
            if kind == 2 and re.fullmatch(r".+:[0-9a-f-]{36}", name):
                original_hosts.add(name[:-37])
        for key, new in new_index.items():
            old = old_index.get(key)
            if old == new:
                continue
            b, namespace, name, tasktype, kind, ack = queue_info(new)
            if old is None:
                require(phase == "postinstall" and startup is not None and namespace in namespaces
                        and new["range_id"] == 1 and tasktype == 1 and kind == 2 and ack == 0,
                        "added queue is not empty owned startup STICKY")
                require(proto_time(scalar(b, 6, 2)) > proto_time(scalar(b, 7, 2)), "STICKY expiry is not after update")
                worker = startup["processes"]["worker"]["pid"]
                hosted = re.fullmatch(r"(.+):([0-9a-f-]{36})", name)
                worker_name = re.fullmatch(r"devflow-" + str(worker) + r"-([0-9]+)-([0-9a-f]{32})", name)
                require(hosted is not None and hosted[1] in original_hosts or worker_name is not None,
                        "STICKY name is not bound to original host/new worker")
                if hosted:
                    require(str(uuid.UUID(hosted[2])) == hosted[2], "invalid host STICKY UUID")
                else:
                    born = datetime.strptime(startup["processes"]["worker"]["birth"],
                                             "%a %b %d %H:%M:%S %Y").replace(tzinfo=UTC).timestamp()
                    require(int(worker_name[1]) >= int(born * 1_000_000_000), "worker STICKY clock predates startup")
                    uuid.UUID(hex=worker_name[2])
                continue
            exact_except(old, new, {"range_id", "data"}, table)
            a, ns, oldname, oldtype, oldkind, oldack = queue_info(old)
            require((ns, oldname, oldtype, oldkind) == (namespace, name, tasktype, kind), "queue semantic identity changed")
            require(0 <= new["range_id"] - old["range_id"] <= (1 if phase == "preinstall" else 2),
                    "unsupported queue lease advance")
            require(ack >= oldack and proto_time(scalar(b, 7, 2)) >= proto_time(scalar(a, 7, 2)),
                    "queue ACK/update time decreased")
            require({k: v for k, v in a.items() if k not in {5, 7, 9}}
                    == {k: v for k, v in b.items() if k not in {5, 7, 9}}, "queue protobuf identity changed")
    elif table == "cluster_metadata_info":
        require(phase == "postinstall", "cluster metadata changed before install")
        for old, new in pairs(before, after, ("metadata_partition", "cluster_name"), table):
            if old == new:
                continue
            exact_except(old, new, {"data", "version"}, table)
            require(new["version"] == old["version"] + 2, "unsupported cluster SQL version change")
            a, b = (message(row, {1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 13}) for row in (old, new))
            exact_except(a, b, {4, 5, 13}, "cluster metadata")
            va, vb = (wire(scalar(p, 4, 2), {1, 2, 4, 5}) for p in (a, b))
            exact_except(va, vb, {5}, "cluster VersionInfo")
            require(proto_time(scalar(vb, 5, 2)) > proto_time(scalar(va, 5, 2)), "cluster version clock did not advance")
            def attributes(data):
                return proto_map(wire(data, {1}), 1, 0)
            require(proto_map(a, 5, 2, attributes) == proto_map(b, 5, 2, attributes),
                    "cluster search attribute maps changed")
            loopback(scalar(b, 13, 2))
    else:
        raise Refusal(f"unproved Temporal table transition: {table}")


def scanner_rows(nodes, trees, startup, system_namespace):
    # Reuse the installed public History descriptor; no provider or RPC import.
    from google.protobuf.message import DecodeError
    from temporalio.api.history.v1 import History

    require(len(nodes) == len(trees) == 2, "startup scanner node/tree set is incomplete")
    tree_index = {(canonical(r["tree_id"]), canonical(r["branch_id"])): r for r in trees}
    require(len(tree_index) == len(trees), "duplicate startup scanner branch")
    scanner_ids = set()
    process = startup["processes"]["temporal"]
    born = datetime.strptime(process["birth"], "%a %b %d %H:%M:%S %Y").replace(tzinfo=UTC).timestamp()
    for node in nodes:
        require(node["shard_id"] == 1 and node["node_id"] == 1 and node["prev_txn_id"] == 0
                and node["data_encoding"] == "Proto3", "invalid startup scanner node")
        history = History()
        try:
            history.ParseFromString(blob(node["data"]))
        except DecodeError as exc:
            raise Refusal("invalid startup scanner protobuf") from exc
        require(blob(node["data"]) == history.SerializeToString(deterministic=True),
                "scanner event BLOB differs from exact producer serialization")
        known = History()
        known.CopyFrom(history)
        known.DiscardUnknownFields()
        require(known.SerializeToString(deterministic=True) == history.SerializeToString(deterministic=True)
                and len(history.events) == 1, "unknown field or extra startup scanner event")
        event = history.events[0]
        attrs = event.workflow_execution_started_event_attributes
        workflow = attrs.workflow_id
        require(workflow in {"temporal-sys-tq-scanner", "temporal-sys-history-scanner"}
                and workflow not in scanner_ids, "unknown or duplicate startup scanner workflow")
        scanner_ids.add(workflow)
        tree_uuid = str(uuid.UUID(bytes=blob(node["tree_id"])))
        branch_uuid = str(uuid.UUID(bytes=blob(node["branch_id"])))
        require(re.fullmatch(str(process["pid"]) + r"@[^@]+@", attrs.identity) is not None,
                "scanner startup identity does not match new Temporal PID")
        require(event.event_time.seconds >= born and node["txn_id"] == -event.task_id - 1
                and event.task_id > 0, "scanner event clock/task does not match new startup")
        expected = History()
        item = expected.events.add()
        item.event_id, item.event_type, item.task_id = 1, 1, event.task_id
        item.event_time.CopyFrom(event.event_time)
        target = item.workflow_execution_started_event_attributes
        target.workflow_type.name = workflow + "-workflow"
        target.task_queue.name, target.task_queue.kind = workflow + "-taskqueue-0", 1
        target.workflow_execution_timeout.SetInParent()
        target.workflow_run_timeout.seconds = 432000 if workflow == "temporal-sys-tq-scanner" else 0
        target.workflow_task_timeout.seconds = 10
        target.initiator, target.attempt = 3, 1
        target.original_execution_run_id = target.first_execution_run_id = tree_uuid
        target.identity, target.workflow_id, target.cron_schedule = attrs.identity, workflow, "0 */12 * * *"
        target.first_workflow_task_backoff.seconds = 43200 - event.event_time.seconds % 43200
        target.header.SetInParent()
        require(history == expected, "startup scanner event differs from exact CRON producer contract")
        tree = tree_index.pop((canonical(node["tree_id"]), canonical(node["branch_id"])), None)
        require(tree is not None and tree["shard_id"] == 1, "scanner node/tree branch mismatch")
        fields = message(tree, {1, 2, 3, 4})
        branch_data = scalar(fields, 1, 2)
        branch = wire(branch_data, {1, 2})
        require(scalar(branch, 1, 2).decode() == tree_uuid
                and scalar(branch, 2, 2).decode() == branch_uuid
                and scalar(fields, 4, 2) == branch_data
                and scalar(fields, 3, 2).decode() == f"{system_namespace}:{workflow}:{tree_uuid}"
                and proto_time(scalar(fields, 2, 2)) >= (event.event_time.seconds, event.event_time.nanos),
                "scanner tree/token/namespace/fork custody mismatch")
    require(not tree_index, "unmatched startup scanner tree")


class SavedCustody:
    def __init__(self, root, baseline_sha256):
        self.root = Path(root).resolve()
        self.files = {}
        self.baseline = self.load("baseline.json", baseline_sha256)
        owners = self.baseline["owners"]
        require(isinstance(owners, list) and len(owners) == 6
                and {str(o["owner"]) for o in owners} == OWNERS, "incomplete baseline owner set")
        self.original = {}
        self.schema = {}
        self.cache = {}
        self.authenticated = set()
        self.statistics = {"databases": 13, "workflow_tables": 312,
                           "temporal_tables": 288, "registry_tables": 9,
                           "changed_tables": 0, "changed_rows": 0}
        for owner in owners:
            for kind, count in (("workflow", 52), ("temporal", 48)):
                self.register((str(owner["owner"]), kind), owner[kind], count)
        self.register(("registry", "registry"), self.baseline["registry"], 9)

    def load(self, name, expected=None):
        path = self.root / name
        require(path.resolve() == path and path.is_file() and not path.is_symlink()
                and path.is_relative_to(self.root), f"unsafe saved receipt: {name}")
        value = path.read_bytes()
        actual = hashlib.sha256(value).hexdigest()
        require(expected is None or actual == expected, f"saved receipt hash changed: {name}")
        self.files[name] = actual
        return json.loads(value)

    def register(self, key, record, count):
        require(len(record["identity"]["tables"]) == count, f"{key}: incomplete table inventory")
        path = Path(record["backup"])
        require(path.parent == self.root / "rollback" and path.resolve() == path
                and path.is_file() and not path.is_symlink(), f"{key}: backup is not frozen rollback data")
        self.original[key] = record

    def authenticate(self, key):
        if key in self.authenticated:
            return
        record = self.original[key]
        path = Path(record["backup"])
        original_sha = sha(path)
        require(original_sha == record["sha256"], f"{key}: original backup hash changed")
        for filename, identity in record.get("copied_files", {}).items():
            copied = Path(filename)
            require(copied.parent == path.parent and copied.resolve() == copied
                    and copied.is_file() and not copied.is_symlink(), f"{key}: unsafe backup member")
            require((original_sha if copied == path else sha(copied)) == identity["sha256"],
                    f"{key}: original backup member changed")
        wal = Path(str(path) + "-wal")
        require(not wal.exists() or wal.stat().st_size == 0,
                f"{key}: unsealed committed WAL needs an authenticated immutable capture")
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)) as db:
            schema = [list(row) for row in db.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")]
            require(schema == record["identity"]["schema"], f"{key}: original schema changed")
            for name, identity in record["identity"]["tables"].items():
                quoted = '"' + name.replace('"', '""') + '"'
                columns = [row[1] for row in db.execute("PRAGMA table_info(" + quoted + ")")]
                require(columns == identity["columns"], f"{key}/{name}: original columns changed")
        self.authenticated.add(key)

    def rows(self, key, table):
        cachekey = key, table
        if cachekey not in self.cache:
            self.authenticate(key)
            record = self.original[key]
            require(table in record["identity"]["tables"], f"{key}: unknown table {table}")
            quoted = '"' + table.replace('"', '""') + '"'
            with closing(sqlite3.connect(Path(record["backup"]).as_uri()
                                        + "?mode=ro&immutable=1", uri=True)) as db:
                result = Counter(canonical(raw_row(row)) for row in db.execute("SELECT * FROM " + quoted))
            identity = record["identity"]["tables"][table]
            require(digest_rows(result) == {k: identity[k] for k in ("rows", "sha256")},
                    f"{key}/{table}: original row membership does not match baseline")
            self.cache[cachekey] = result
        return self.cache[cachekey].copy()

    def reconstruct(self, key, current):
        require(set(current) == {"identity", "raw_changes"}, f"{key}: unsupported custody shape")
        before = self.original[key]["identity"]
        after = current["identity"]
        require(set(before) == set(after) == {"schema", "tables"}
                and before["schema"] == after["schema"]
                and before["tables"].keys() == after["tables"].keys(), f"{key}: schema/table set changed")
        deltas = current["raw_changes"]
        require(isinstance(deltas, dict) and deltas.keys() <= before["tables"].keys(),
                f"{key}: unrecognized delta table")
        reconstructed = {}
        for table, identity in before["tables"].items():
            actual = after["tables"][table]
            require(set(identity) == set(actual) == {"rows", "sha256", "columns"}
                    and identity["columns"] == actual["columns"], f"{key}/{table}: column set changed")
            require(type(actual["rows"]) is int and actual["rows"] >= 0
                    and isinstance(actual["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", actual["sha256"]),
                    f"{key}/{table}: invalid table identity")
            if table not in deltas:
                require(identity == actual, f"{key}/{table}: changed hash/count without raw rows")
                continue
            delta = deltas[table]
            require(set(delta) == {"removed", "added"}
                    and all(isinstance(delta[k], list) for k in delta), f"{key}/{table}: malformed delta")
            require(any(delta.values()), f"{key}/{table}: empty delta")
            width = len(identity["columns"])
            require(all(isinstance(row, list) and len(row) == width
                        for part in delta.values() for row in part), f"{key}/{table}: invalid row shape")
            for part in delta.values():
                for row in part:
                    for value in row:
                        require(value is None or type(value) in {int, float, str, dict},
                                f"{key}/{table}: value is not a raw SQLite cell")
                        if isinstance(value, dict):
                            blob(value)
            removed, added = (Counter(canonical(row) for row in delta[k]) for k in ("removed", "added"))
            require(not (removed & added), f"{key}/{table}: nonminimal raw delta")
            original = self.rows(key, table)
            require(not (removed - original), f"{key}/{table}: removed row is absent from original membership")
            result = original - removed + added
            require(digest_rows(result) == {k: actual[k] for k in ("rows", "sha256")},
                    f"{key}/{table}: full reconstructed rows/hash/count mismatch")
            reconstructed[table] = result
            self.statistics["changed_tables"] += 1
            self.statistics["changed_rows"] += sum(added.values())
        return reconstructed

    def dict_rows(self, key, table, rows=None):
        columns = self.original[key]["identity"]["tables"][table]["columns"]
        values = self.rows(key, table) if rows is None else rows
        return [dict(zip(columns, json.loads(row), strict=True)) for row in values.elements()]

    def snapshot(self, name):
        value = self.load(name)
        require(set(value) == {"owners", "registry"} and set(value["owners"]) == OWNERS,
                "incomplete current owner/registry custody")
        result = {}
        for owner, kinds in value["owners"].items():
            require(set(kinds) == {"workflow", "temporal"}, f"owner {owner}: missing workflow/Temporal custody")
            for kind, record in kinds.items():
                result[owner, kind] = self.reconstruct((owner, kind), record)
        result["registry", "registry"] = self.reconstruct(("registry", "registry"), value["registry"])
        require(not result["registry", "registry"], "execution ownership registry changed")
        return value, result

    def workflow(self, owner, reconstructed, phase, generation=None):
        key = owner, "workflow"
        allowed = {"delivery_closed_projections", "delivery_closure_observations",
                   "delivery_pr_observations", "delivery_features", "delivery_feature_events",
                   "delivery_project_outbox", "delivery_feature_views", "sqlite_sequence"}
        if phase == "postinstall" and "delivery_feature_projections" in reconstructed:
            require(generation is not None and re.fullmatch(r"[0-9a-f]{64}", generation),
                    "projection changed without generation computed from the qualified installed source")
            allowed.add("delivery_feature_projections")
            for before, after in pairs(self.dict_rows(key, "delivery_feature_projections"), self.dict_rows(
                    key, "delivery_feature_projections", reconstructed["delivery_feature_projections"]),
                    ("run_id",), "feature projection"):
                if before != after:
                    exact_except(before, after, {"generation", "projected_at"}, "feature projection")
                    require(after["generation"] == generation, "projection does not match qualified source generation")
                    advance(before, after, "projected_at")
        require(set(reconstructed) <= allowed, f"owner {owner}: unproved workflow transition: "
                + ",".join(sorted(set(reconstructed) - allowed)))
        for table, rows in reconstructed.items():
            if table not in {"delivery_closed_projections", "delivery_closure_observations"}:
                continue
            before = self.dict_rows(key, table)
            after = self.dict_rows(key, table, rows)
            changed = [(a, b) for a, b in pairs(before, after,
                ("run_id", "workflow_id") if table == "delivery_closed_projections" else ("workflow_id",), table)
                if a != b]
            clock_rows([a for a, _ in changed], [b for _, b in changed],
                       ("run_id", "workflow_id") if table == "delivery_closed_projections" else ("workflow_id",),
                       {300} if table == "delivery_closed_projections" else {300, 3600}, table)

    def temporal(self, owner, reconstructed, phase, startup=None):
        allowed = {"shards", "task_queues"} if phase == "preinstall" else {
            "shards", "task_queues", "cluster_metadata_info", "history_node", "history_tree"}
        require(set(reconstructed) <= allowed, f"owner {owner}: unproved Temporal transition: "
                + ",".join(sorted(set(reconstructed) - allowed)))
        if not reconstructed:
            return
        key = owner, "temporal"
        namespaces = self.dict_rows(key, "namespaces")
        namespace_ids = {str(uuid.UUID(bytes=blob(row["id"]))) for row in namespaces}
        if phase == "postinstall":
            require(startup is not None, f"owner {owner}: missing saved new process identities")
            old = next(o for o in self.baseline["owners"] if str(o["owner"]) == owner)["public"]["lifecycle"]["processes"]
            for role in ("temporal", "worker"):
                new = startup["processes"][role]
                require(type(new["pid"]) is int and new["pid"] > 0
                        and (new["pid"], new["birth"]) != (old[role]["pid"], old[role]["identity"]),
                        "startup receipt reused an original process identity")
        for table, rows in reconstructed.items():
            if table in {"history_node", "history_tree"}:
                continue
            temporal_rows(table, self.dict_rows(key, table), self.dict_rows(key, table, rows),
                          phase, startup, namespace_ids)
        if "history_node" in reconstructed or "history_tree" in reconstructed:
            require({"history_node", "history_tree"} <= reconstructed.keys(), "incomplete scanner node/tree capture")
            additions = {}
            for table in ("history_node", "history_tree"):
                original = self.rows(key, table)
                require(not (original - reconstructed[table]), "original Temporal history removed/changed")
                additions[table] = self.dict_rows(key, table, reconstructed[table] - original)
                original_branches = {(canonical(r["tree_id"]), canonical(r["branch_id"]))
                                     for r in self.dict_rows(key, table)}
                require(all((canonical(r["tree_id"]), canonical(r["branch_id"])) not in original_branches
                            for r in additions[table]), "scanner addition reused an original branch")
            system = [str(uuid.UUID(bytes=blob(row["id"]))) for row in namespaces if row["name"] == "temporal-system"]
            require(len(system) == 1, "original Temporal system namespace is missing/ambiguous")
            scanner_rows(additions["history_node"], additions["history_tree"], startup, system[0])

    def refresh(self, reconstructed):
        tables = ("delivery_pr_observations", "delivery_features", "delivery_project_outbox",
                  "delivery_feature_views")
        state = {}
        for owner in sorted(OWNERS):
            key = owner, "workflow"
            state[owner] = {}
            for table in tables:
                before = self.dict_rows(key, table)
                after = self.dict_rows(key, table, reconstructed[key].get(table, self.rows(key, table)))
                state[owner][table] = before, after
        outboxes = [{}, {}]
        changed_view_issues = set()
        for owner, records in state.items():
            observations = {r["url"]: r for r in records["delivery_pr_observations"][1]}
            for before, after in pairs(*records["delivery_pr_observations"], ("url",), "PR observation"):
                if before == after:
                    continue
                exact_except(before, after, {"checked_at", "next_check_at"}, "PR observation")
                for row in (before, after):
                    require(row["error"] is None and row["observation_json"] is not None,
                            "refresh lacks unchanged successful PR observation")
                    require((stamp(row["next_check_at"]) - stamp(row["checked_at"])).total_seconds()
                            == 86400, "PR refresh interval changed")
                advance(before, after, "checked_at")
                advance(before, after, "next_check_at")
            changed_features = {}
            for before, after in pairs(*records["delivery_features"], ("issue",), "feature"):
                if before == after:
                    continue
                exact_except(before, after, {"version", "payload_json", "updated_at"}, "feature")
                require(type(after["version"]) is int and after["version"] == before["version"] + 1,
                        "feature version is not a single proved refresh")
                advance(before, after, "updated_at")
                require(producer_json(json.loads(after["payload_json"])) == after["payload_json"],
                        "feature payload is not exact producer serialization")
                require(pr_payload(json.loads(before["payload_json"]), json.loads(after["payload_json"]),
                                   observations, "feature"), "feature version advanced without PR refresh")
                changed_features[after["issue"]] = after
            key = owner, "workflow"
            events_before = self.rows(key, "delivery_feature_events")
            events_after = reconstructed[key].get("delivery_feature_events", events_before)
            require(not (events_before - events_after), "original feature event removed")
            appended = self.dict_rows(key, "delivery_feature_events", events_after - events_before)
            require(len(appended) == len(changed_features), "unbound appended feature event")
            previous_sequence = max((r["sequence"] for r in self.dict_rows(key, "delivery_feature_events")), default=0)
            require(sorted(r["sequence"] for r in appended)
                    == list(range(previous_sequence + 1, previous_sequence + 1 + len(appended))),
                    "feature event sequence is not contiguous")
            for event in appended:
                feature = changed_features.pop(event["issue"], None)
                require(feature is not None and event == {
                    "sequence": event["sequence"], "issue": feature["issue"], "version": feature["version"],
                    "payload_json": feature["payload_json"], "created_at": feature["updated_at"]},
                    "feature event does not equal authoritative refreshed feature")
            for before, after in pairs(self.dict_rows(key, "sqlite_sequence"), self.dict_rows(
                    key, "sqlite_sequence", reconstructed[key].get("sqlite_sequence", self.rows(key, "sqlite_sequence"))),
                    ("name",), "SQLite sequence"):
                require(before == after or (before["name"] == "delivery_feature_events"
                        and appended and after["seq"] == before["seq"] + len(appended)
                        and after["seq"] == max(r["sequence"] for r in appended)), "unbound SQLite sequence advance")
            for before, after in pairs(*records["delivery_project_outbox"], ("issue",), "Project outbox"):
                if before != after:
                    exact_except(before, after, {"identity", "payload_json", "checked_at", "next_attempt_at"},
                                 "Project outbox")
                    require(before["state"] == "consistent", "Project refresh state is not consistent")
                    for row in (before, after):
                        require((stamp(row["next_attempt_at"]) - stamp(row["checked_at"])).total_seconds()
                                == 86400, "Project refresh interval changed")
                    advance(before, after, "checked_at")
                    advance(before, after, "next_attempt_at")
                for index, row in enumerate((before, after)):
                    payload = json.loads(row["payload_json"])
                    require(row["identity"] == outbox_identity(payload), "Project identity differs from producer formula")
                    if before["payload_json"] != after["payload_json"]:
                        candidates = [r for r in records["delivery_features"][index] if r["issue"] == row["issue"]]
                        require(len(candidates) == 1, "changed Project payload lacks authoritative feature")
                        feature = candidates[0]
                        expected = {**json.loads(feature["payload_json"]), "version": feature["version"],
                                    "source": next(o["tracking_db"] for o in self.baseline["owners"]
                                                   if str(o["owner"]) == owner)}
                        require(payload == expected and row["payload_json"] == producer_json(expected),
                                "Project payload differs from authoritative feature/source/version")
                    require(row["issue"] not in outboxes[index], "ambiguous selected Project owner")
                    outboxes[index][row["issue"]] = row
            for before, after in pairs(*records["delivery_feature_views"], ("issue",), "feature view"):
                if before != after:
                    exact_except(before, after, {"payload_json", "updated_at"}, "feature view")
                    advance(before, after, "updated_at")
                    changed_view_issues.add(after["issue"])
        for issue in changed_view_issues:
            for index in (0, 1):
                require(issue in outboxes[index], "view lacks authoritative selected outbox")
                outbox = outboxes[index][issue]
                expected = {**json.loads(outbox["payload_json"]), "mirror": {
                    k: outbox[k] for k in ("state", "last_error", "checked_at", "next_attempt_at")},
                    "project_receipt": json.loads(outbox["receipt_json"] or "null")}
                for records in state.values():
                    views = [r for r in records["delivery_feature_views"][index] if r["issue"] == issue]
                    require(len(views) == 1 and views[0]["payload_json"] == producer_json(expected),
                            "all six feature views must equal authoritative outbox and Project receipt")

    def preservation(self, reconstructed, startup, revision):
        public = self.load("public-readback.json")
        require(isinstance(public, list) and len(public) == 6, "incomplete public readback")
        configs = self.load("config-readback.json")
        require(configs.keys() == self.baseline["configs"].keys(), "config/descriptor file set changed")
        for record in self.baseline["configs"].values():
            backup = Path(record["backup"])
            require(backup.parent == self.root / "rollback" and backup.resolve() == backup
                    and backup.is_file() and not backup.is_symlink(), "config backup is not frozen rollback data")
            require(sha(backup) == record["sha256"], "frozen config/plist/activation backup changed")
        state_roots = {str(o["owner"]): Path(json.loads(Path(
            self.baseline["configs"][o["config"]]["backup"]).read_bytes())["state_root"])
            for o in self.baseline["owners"]}
        require(self.load("durable-diff.json") == {}, "durable evidence/config/history files changed")
        closed = self.load("closed-readback.json")
        require(closed["temporal"] == self.baseline["temporal_closure"], "closed execution inventory changed")
        native = closed["native"]
        require(native == self.baseline["native_idle"] and native["live_owned"] == 0
                and set(native["native_observer_timezones"]) == {"system-default", "UTC0"},
                "native process/port custody or inventory changed")
        for owner in self.baseline["owners"]:
            index, before = owner["owner"], owner["public"]
            after, identities = public[index], startup[index]
            require(set(before) == set(after) and before["runs"] == after["runs"]
                    and before["histories"].keys() == after["histories"].keys(), "public run/history inventory changed")
            for run_id, old in before["histories"].items():
                new = json.loads(json.dumps(after["histories"][run_id]))
                if old["run"].get("feature") != new["run"].get("feature"):
                    feature = new["run"]["feature"]
                    key = str(index), "workflow"
                    view_rows = self.dict_rows(key, "delivery_feature_views", reconstructed[key].get(
                        "delivery_feature_views", self.rows(key, "delivery_feature_views")))
                    matching = [r for r in view_rows if r["issue"] == feature["issue"]]
                    require(len(matching) == 1 and feature == json.loads(matching[0]["payload_json"])
                            and feature["issue"] == old["run"]["feature"]["issue"],
                            "public feature is not its authenticated refreshed view")
                    new["run"]["feature"] = old["run"]["feature"]
                require(new == old, f"public status/events/evidence changed: {run_id}")
            lifecycle = json.loads(json.dumps(before["lifecycle"]))
            require(set(identities) == {"processes", "worker_ready"}
                    and set(identities["processes"]) == {"api", "worker", "temporal"},
                    "incomplete saved managed process identities")
            for role, process in identities["processes"].items():
                original = before["lifecycle"]["processes"][role]
                require((process["pid"], process["birth"]) != (original["pid"], original["identity"])
                        and process["TZ_present"] is False, "old or TZ-polluted managed process identity")
                lifecycle["processes"][role].update(pid=process["pid"], identity=process["birth"])
            require(after["lifecycle"] == lifecycle, "managed lifecycle descriptor changed outside PID/birth")
            service = json.loads(json.dumps(before["service"]))
            service["pid"] = identities["processes"]["api"]["pid"]
            service["runtime_identity"]["revision"] = revision
            require(after["service"] == service and after["service"]["capacity"]["active"] == 0
                    and not after["service"]["runtime_identity"]["dirty"], "public service/policy/readiness changed")
            for name in ("service-processes.json", "worker-ready.json"):
                matches = [(p, record) for p, record in self.baseline["configs"].items()
                           if p == str(state_roots[str(index)] / name)]
                require(len(matches) == 1, "original lifecycle descriptor backup missing")
                path, record = matches[0]
                raw = Path(record["backup"]).read_bytes()
                require(hashlib.sha256(raw).hexdigest() == record["sha256"], "frozen descriptor backup changed")
                expected = json.loads(raw)
                if name == "service-processes.json":
                    for role, process in identities["processes"].items():
                        expected["processes"][role].update(pid=process["pid"], identity=process["birth"])
                    encoded = (json.dumps(expected, indent=2) + "\n").encode()
                else:
                    expected.update(pid=identities["processes"]["worker"]["pid"],
                                    identity=identities["processes"]["worker"]["birth"])
                    require(expected == identities["worker_ready"], "worker ready changed outside PID/birth")
                    encoded = json.dumps(expected).encode()
                require(configs[path] == hashlib.sha256(encoded).hexdigest(), "current descriptor hash is not exact PID/birth transformation")
        descriptor_paths = {str(state_roots[str(o["owner"])] / name) for o in self.baseline["owners"]
                            for name in ("service-processes.json", "worker-ready.json")}
        sync = self.load("sync-readback.json")
        expected_sync = json.loads(json.dumps(self.baseline["sync"]))
        expected_sync.update(pid=sync["pid"], birth=sync["birth"])
        arguments = expected_sync["plist"]["ProgramArguments"]
        source = str(Path(arguments[0]).parents[3])
        config_path = arguments[arguments.index("--config") + 1]
        activation_path = arguments[arguments.index("--activation-file") + 1]
        config_sha = self.baseline["configs"][config_path]["sha256"]
        token = hashlib.sha256(json.dumps([source, revision, config_path, config_sha]).encode()).hexdigest()
        arguments[arguments.index("--activation-token") + 1] = token
        arguments[arguments.index("--expected-revision") + 1] = revision
        require(arguments[arguments.index("--config-sha256") + 1] == config_sha,
                "synchronizer policy hash changed")
        require(sync == expected_sync and sync["TZ_present"] is False, "synchronizer changed outside exact qualified activation/PID/birth")
        plist_paths = [path for path in self.baseline["configs"] if path.endswith("/com.ebarti.devflow.project-sync.plist")]
        require(len(plist_paths) == 1, "original managed synchronizer plist missing")
        plist_path = plist_paths[0]
        for path, record in self.baseline["configs"].items():
            backup = Path(record["backup"])
            if path not in descriptor_paths:
                expected_sha = record["sha256"]
                if path == activation_path:
                    original = json.loads(backup.read_bytes())
                    require(set(original) == {"token"}, "unknown activation field")
                    expected_sha = hashlib.sha256(json.dumps({"token": token}).encode()).hexdigest()
                elif path == plist_path:
                    original = plistlib.loads(backup.read_bytes())
                    require(original == self.baseline["sync"]["plist"], "original synchronizer plist custody mismatch")
                    expected_sha = hashlib.sha256(plistlib.dumps(expected_sync["plist"])).hexdigest()
                require(configs[path] == expected_sha, "config/plist/activation changed without exact qualified transformation")
        require((sync["pid"], sync["birth"]) != (self.baseline["sync"]["pid"], self.baseline["sync"]["birth"]),
                "synchronizer reused original process identity")


def source_generation(sources):
    # The existing release caller qualifies revision/tree/clean status first.
    if sources is None:
        return None
    sources = [Path(path) for path in sources]
    require(len(sources) == 3 and len(set(sources)) == 3, "expected three qualified source trees")
    generations = set()
    for source in sources:
        package = source / "runtime/src/devflow_temporal"
        require(package.resolve() == package and package.is_dir(), "unsafe qualified source package")
        modules = sorted(package.glob("*.py"))
        require(modules, "empty qualified source package")
        value = hashlib.sha256()
        for module in modules:
            require(module.is_file() and not module.is_symlink(), "unsafe qualified source module")
            value.update(module.name.encode())
            value.update(module.read_bytes())
        generations.add(value.hexdigest())
    require(len(generations) == 1, "qualified installed source generations differ")
    return generations.pop()


def validate(root, snapshot, baseline_sha256, *, phase="preinstall", peer=None, revision=None, generation_sources=None):
    started = time.perf_counter()
    saved = None
    try:
        require(re.fullmatch(r"[0-9a-f]{64}", baseline_sha256 or ""), "missing frozen baseline SHA256")
        require(phase in {"preinstall", "postinstall"}, "unknown phase")
        generation = source_generation(generation_sources)
        saved = SavedCustody(root, baseline_sha256)
        value, reconstructed = saved.snapshot(snapshot)
        startup = saved.load("process-readback.json") if phase == "postinstall" else None
        require(startup is None or isinstance(startup, list) and len(startup) == 6,
                "incomplete saved new process identities")
        for owner in sorted(OWNERS):
            saved.workflow(owner, reconstructed[owner, "workflow"], phase, generation)
            saved.temporal(owner, reconstructed[owner, "temporal"], phase,
                           startup[int(owner)] if startup is not None else None)
        refresh_tables = {"delivery_pr_observations", "delivery_features", "delivery_feature_events",
                          "delivery_project_outbox", "delivery_feature_views", "sqlite_sequence"}
        if any(refresh_tables & reconstructed[owner, "workflow"].keys() for owner in OWNERS):
            saved.refresh(reconstructed)
        for key in saved.original:
            saved.authenticate(key)
        if peer:
            other, _ = saved.snapshot(peer)
            require(canonical(value) == canonical(other), "stopped/current custody differs")
        if phase == "postinstall":
            revision = revision or saved.baseline.get("revision", saved.baseline.get("base"))
            require(re.fullmatch(r"[0-9a-f]{40}", revision or ""), "missing qualified actual revision")
            saved.preservation(reconstructed, startup, revision)
        result = {"status": "PASS", "phase": phase, "statistics": saved.statistics, "findings": []}
    except (Refusal, OSError, KeyError, TypeError, ValueError, sqlite3.Error,
            IndexError, AttributeError, ImportError) as exc:
        message = str(exc) if isinstance(exc, Refusal) else f"invalid custody input ({type(exc).__name__}): {exc}"
        result = {"status": "FAIL", "phase": phase, "findings": [message]}
    result["inputs"] = saved.files if saved is not None else {}
    result["elapsed_seconds"] = round(time.perf_counter() - started, 6)
    result["scope"] = "saved custody preservation only; no fresh observation or release authorization"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path, help="saved release directory containing baseline.json and rollback/")
    parser.add_argument("--snapshot", required=True, help="saved complete custody JSON filename")
    parser.add_argument("--baseline-sha256", required=True, help="SHA256 pinned by the frozen baseline approval")
    parser.add_argument("--phase", choices=("preinstall", "postinstall"), default="preinstall")
    parser.add_argument("--peer", help="second custody filename that must be canonically identical")
    parser.add_argument("--revision", help="actual source-qualified installed revision (defaults to baseline revision)")
    parser.add_argument("--generation-source", action="append", type=Path,
                        help="repeat for all three source trees already qualified by the release caller")
    args = parser.parse_args()
    result = validate(args.root, args.snapshot, args.baseline_sha256, phase=args.phase, peer=args.peer, revision=args.revision, generation_sources=args.generation_source)
    print(canonical(result))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
