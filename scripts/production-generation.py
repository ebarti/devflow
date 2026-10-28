#!/usr/bin/env python3.12
"""Explicit immutable production source rollover; all recovery uses retained stdlib code.

Preparation owns a separate final source/Git/environment. Preflight emits the
exact request which apply accepts. Neither operation edits execution snapshots,
run rows, budgets or earlier installation acknowledgements. An unsupported
custody fact is a refusal, never an inferred completed transition.
"""

from __future__ import annotations

import base64
import contextlib
import fcntl
import hashlib
import importlib.util
import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "devflow.production-source-successor/v2"
ANSWER = "Apply this exact stopped source successor; preserve original authority and budgets"
TERMINAL = {"verified", "rolled_back"}
LIMIT = 4 * 1024 * 1024
MAX_ROWS = 8192
CONTROL = ("production-generation.py", "production-layout.py", "production-entry.py",
           "production-source.py", "installation-paths.py", "install-rollback.py", "install-production.py")


def load(name, root=ROOT):
    spec = importlib.util.spec_from_file_location(name, root / "scripts" / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def encoded(value):
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if len(raw) > LIMIT:
        raise ValueError("transition_evidence_oversized: explicit evidence exceeds 4 MiB")
    return raw


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def refused(code, fact):
    raise ValueError(code + ": " + fact)


def device(path):
    return Path(path).stat().st_dev


def bound_json(paths, file, expected=None):
    """Bind private bytes and the actual opened preimage before deciding effects."""
    raw = paths.read_bytes(file)
    saved = {}
    identity = load("install-rollback").identity(file, saved=saved)
    if identity.get("sha256") != sha(raw):
        refused("source_preimage_changed", "installation evidence changed while binding its identity")
    value = json.loads(raw)
    if expected is not None and value != expected:
        refused("source_preimage_changed", "installation evidence changed after its owner observation")
    return value, raw, identity


def owner_path():
    return Path.home().resolve(strict=True) / ".local/share/devflow"


@contextlib.contextmanager
def stopped_owner(paths, *, recovery=False):
    owner = owner_path()
    paths.protected_directory(owner)
    descriptor = os.open(owner / ".production-install.lock", os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        paths._CONTROLLER_FD = descriptor
        update = owner / ".production-update.json"
        if (not recovery and update.exists()
                and paths.read_json(update).get("phase") not in TERMINAL):
            refused("source_transition_unfinished", "replay the retained transition recovery command")
        yield descriptor
    finally:
        paths._CONTROLLER_FD = None
        os.close(descriptor)


def current(paths):
    owner = owner_path()
    receipt = paths.read_json(owner / ".production-install.json")
    if receipt.get("managed_owner_version") != 1 or receipt.get("phase") != "verified":
        refused("legacy_owner_custody_unknown", "owner was not born under the complete production protocol")
    active = owner / ".production-active.json"
    root = Path(paths.read_json(active)["root"]) if active.exists() else owner / "developer-workflow"
    paths.layout_helper().installed(root)
    if not paths.same_node(receipt.get("target_identity"), paths.node(root)):
        refused("source_owner_changed", "current source object differs from the authenticated owner")
    inventory = paths.read_inventory(root, receipt)
    if inventory.get("new_owner") is not True or receipt.get("born_new_owner") is not True:
        refused("legacy_owner_custody_unknown", "historical configuration census cannot be inferred")
    return root, receipt, inventory


def inspect_runtime(paths, root):
    return (paths.runtime(root) if root == owner_path() / "developer-workflow"
            else paths.generation_runtime(root))


def database(path, *, connection=None):
    """Hash every retained logical row through one read-only SQLite snapshot."""
    result, runs, total = {}, [], 0
    context = (sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
               if connection is None else contextlib.nullcontext(connection))
    with context as db:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        schema = [tuple(row) for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master "
                                                  "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")]
        result["schema"] = schema
        for kind, table, _name, _sql in schema:
            if kind != "table":
                continue
            quoted = '"' + table.replace('"', '""') + '"'
            cursor = db.execute("SELECT * FROM " + quoted + " LIMIT ?", (MAX_ROWS + 1,))
            names = [item[0] for item in cursor.description]
            rows = cursor.fetchall()
            total += len(rows)
            if total > MAX_ROWS:
                refused("tracking_evidence_oversized", "source successor requires a bounded complete tracking snapshot")
            values = [{name: ({"sqlite_blob_base64": base64.b64encode(value).decode()}
                              if isinstance(value, bytes) else value)
                       for name, value in zip(names, row, strict=True)} for row in rows]
            result[table] = sorted(values, key=lambda row: encoded(row))
            if table == "delivery_runs":
                runs = values
        if "delivery_runs" not in result:
            refused("tracking_schema_unknown", "registered initialized store lacks its delivery schema")
        return {"sha256": sha(encoded(result)), "runs": runs}


def observed_database(root, path, descriptor):
    # Reuse #240's bounded complete DB/WAL copy. A mode=ro source connection
    # alone can create a missing SHM sidecar, which is not observational.
    program = '''
import importlib.util,json,pathlib,sys
from devflow_temporal.delivery_execution_config import observation_mode
from devflow_temporal.delivery_store import _observation_tracking_connection
s=importlib.util.spec_from_file_location('generation_observer',sys.argv[1])
g=importlib.util.module_from_spec(s);s.loader.exec_module(g)
p=pathlib.Path(sys.argv[2])
with observation_mode(),_observation_tracking_connection(p) as db:
 result=g.database(p,connection=db)
print(json.dumps(result,sort_keys=True))
'''
    result = subprocess.run([str(root / "runtime/.venv/bin/python"), "-I", "-B", "-c", program,
                             str(Path(__file__).resolve()), str(path)], capture_output=True, timeout=90,
                            env={**os.environ, "DEVFLOW_INSTALLATION_PROBE_FD": str(descriptor)},
                            pass_fds=(descriptor,))
    if result.returncode or len(result.stdout) > LIMIT:
        refused("tracking_snapshot_unavailable", result.stderr.decode(errors="replace")[-4096:])
    return json.loads(result.stdout)


def same_volume_destinations(prepared, owners, *, registrations=()):
    """Read-only device check; actual exchange probes belong to executing apply."""
    def existing_parent(path):
        parent = Path(path).parent
        while not parent.exists():
            parent = parent.parent
        return parent

    backup_volume = device(load("install-rollback").backup_anchor(Path(prepared["home"])))
    destinations = [owner_path() / ".production-install.json", owner_path() / ".production-active.json"]
    destinations += [Path(prepared["skills"]) / "reserved-registration"]
    destinations += [Path(name) for name in registrations]
    destinations += [Path(item["path"]) for item in owners]
    destinations += [Path(item["before"]["installation_namespace"]["witness"])
                     for item in owners if "namespace_witness" in item]
    for destination in destinations:
        if device(existing_parent(destination)) != backup_volume:
            refused("transition_filesystem_unsupported",
                    "all guarded registration/configuration/authority effects require the backup volume: " + str(destination))


def registration_plan(before, staged, prepared):
    """Use the complete install planner and bind every current/future target."""
    entry = load("install-service-entry", staged)
    skills, home = Path(prepared["skills"]), Path(prepared["home"])
    entry.plan(skills, home, False, migration_source=before, predecessor=before)
    recovery = load("install-rollback")
    targets = sorted(set(recovery.target_paths(before, skills, home))
                     | set(recovery.target_paths(staged, skills, home)), key=str)
    identities = {str(path): recovery.identity(path) for path in targets}
    entry.plan(skills, home, False, migration_source=before, predecessor=before)
    if any(recovery.identity(Path(path)) != identity for path, identity in identities.items()):
        refused("registration_preimage_changed", "registration changed during complete read-only planning")
    return identities


def registered_databases(paths, inventory, *, observer=None, descriptor=None):
    databases = {}
    for records in inventory["configurations"].values():
        for record in records:
            databases.setdefault(record["tracking_db"], set()).add(record["state_root"])
    for name in ("store_observations", "store_births"):
        for path, record in inventory[name].items():
            databases.setdefault(path, set()).update(record["state_roots"])
    observations = {}
    for name, states in sorted(databases.items()):
        path = paths.persistence_path(Path(name), "registered tracking database")
        for state in states:
            folder = paths.persistence_path(Path(state), "registered persistence root") / "runtime-installations"
            if any(os.path.lexists(folder / item) for item in ("current.json", "pending.json")):
                refused("native_installation_successor_unknown",
                        "retained runtime-installation acknowledgement needs its versioned native successor")
        if not path.exists():
            before = inventory["store_observations"].get(name)
            if (before and before.get("first_observed_absent") is True
                    and name not in inventory["store_births"]):
                observations[name] = {"absent_before_any_writer": True}
                continue
            refused("tracking_store_unavailable", "a missing admitted store remains unknown: " + name)
        observed = (database(path) if observer is None else observed_database(observer, path, descriptor))
        for row in observed["runs"]:
            spec = json.loads(row["request_json"])
            if spec.get("provider") != "fake" or spec.get("policy", {}).get("native_identity") is not None:
                refused("native_descendant_custody_unknown",
                        "native admitted authority requires independent closure and explicit successor acknowledgement (#242)")
        observations[name] = observed
    return observations


def capture_cohorts(root, owners, observations, descriptor, before_root, receipt_sha256):
    """Use the actual prepared SDK and its existing authentic closure readers."""
    selected = [owner for owner in owners if observations[owner["tracking_db"]].get("runs")]
    if not selected:
        return []
    payload = {"owners": selected, "configured_paths": [owner["path"] for owner in owners],
               "source_owner": {"root": str(before_root), "receipt_sha256": receipt_sha256},
               "descriptor": descriptor}
    program = ("import json,sys;from contextlib import ExitStack;"
               "from devflow_temporal.delivery_config import DeliveryConfig;"
               "from devflow_temporal.delivery_runtime_successor import capture_owner;"
               "p=json.load(sys.stdin);out=[];\n"
               "with ExitStack() as leases:\n"
               " for o in p['owners']:\n"
               "  c=DeliveryConfig._from_raw(__import__('pathlib').Path(o['path']),o['before']);"
               "out.append(capture_owner(c,o,configured_paths=p['configured_paths'],leases=leases,"
               "source_owner=p['source_owner'],descriptor=p['descriptor']))\n"
               " print(json.dumps(out,sort_keys=True))")
    result = subprocess.run([str(root / "runtime/.venv/bin/python"), "-I", "-B", "-c", program],
                            input=encoded(payload), capture_output=True, timeout=90,
                            env={**os.environ, "DEVFLOW_INSTALLATION_PROBE_FD": str(descriptor)},
                            pass_fds=(descriptor,))
    if result.returncode:
        refused("closed_cohort_custody_unknown", result.stderr.decode(errors="replace")[-4096:])
    if len(result.stdout) > LIMIT:
        refused("transition_evidence_oversized", "closed SDK evidence exceeds the explicit bound")
    return json.loads(result.stdout)


def evidence(paths, prepared, descriptor):
    before_root, receipt, inventory = current(paths)
    staged = Path(prepared["root"])
    allocation = paths.layout_helper().prepared(staged)
    if allocation["allocation_sha256"] != prepared["allocation_sha256"]:
        refused("prepared_generation_changed", "allocation changed after preparation")
    proof = paths.generation_runtime(staged)
    if proof != prepared["proof"]:
        refused("prepared_generation_changed", "source, environment or imports changed after preparation")
    if str(before_root) != prepared["predecessor_root"]:
        refused("source_predecessor_changed", "prepare a successor of the currently active source")
    # No source or environment is changed while this EX is held. Every owner
    # admitted by a foundation-born source holds the corresponding SH before
    # operational access, including a process imported before EX acquisition.
    previous = paths.production_runtime(before_root)
    if previous != receipt.get("proof"):
        refused("source_predecessor_changed", "actual previous source/environment differs from its verified receipt")
    observations = registered_databases(paths, inventory, observer=before_root, descriptor=descriptor)
    owners = []
    if len(inventory["configurations"]) > 16:
        refused("owner_evidence_oversized", "explicit complete configuration census exceeds 16 owners")
    for name, registrations in sorted(inventory["configurations"].items()):
        file = Path(name)
        before, raw, before_identity = bound_json(paths, file)
        checked = paths.configuration(file)
        record = {"config_sha256": sha(raw), "state_root": checked["state_root"],
                  "tracking_db": checked["tracking_db"]}
        if record not in registrations:
            refused("configuration_custody_unknown", "current configuration was not registered by its source owner")
        after = dict(before, helpers_dir=str(staged / "skills/devflow/scripts"))
        rows = observations[record["tracking_db"]].get("runs", [])
        if before.get("provider", "codex") == "codex":
            if rows:
                refused("native_successor_unknown", "native frozen execution grants require the native successor protocol")
            binaries = list((staged / "runtime/.venv/lib").glob("python*/site-packages/codex_cli_bin/bin/codex"))
            if len(binaries) != 1:
                refused("bundled_cli_unknown", "prepared generation does not contain one measured bundled CLI")
            after["codex_bin"] = str(binaries[0])
        owned = [row for row in rows if json.loads(row["request_json"])["config_path"] == name]
        item = {"path": name, "before": before, "before_sha256": sha(raw), "before_identity": before_identity, "after": after,
                       "tracking_db": record["tracking_db"], "state_root": record["state_root"],
                       "runs": sorted(row["run_id"] for row in owned)}
        namespace = before.get("installation_namespace")
        if namespace is not None:
            witness_file = Path(namespace["witness"])
            witness, witness_raw, witness_identity = bound_json(paths, witness_file)
            if (namespace.get("version") != 1 or namespace.get("namespace_id") != file.parent.name
                    or file.name != "config.json" or witness_file != file.parent / "witness.json"
                    or witness.get("phase") != "verified" or witness.get("root") != str(before_root)
                    or witness.get("config_sha256") != sha(raw)
                    or not paths.same_node(witness["namespace_identity"], paths.node(file.parent))):
                refused("namespace_custody_unknown", "source successor found an unauthenticated namespace")
            item.update(namespace_witness=witness, namespace_witness_sha256=sha(witness_raw),
                        namespace_witness_identity=witness_identity)
        owners.append(item)
    configured = {owner["path"] for owner in owners}
    for observation in observations.values():
        if any(json.loads(row["request_json"])["config_path"] not in configured
               for row in observation.get("runs", [])):
            refused("owner_namespace_unknown", "not every original owner sharing the store is enrolled")
    _receipt, receipt_raw, receipt_identity = bound_json(paths, owner_path() / ".production-install.json", receipt)
    _inventory, inventory_raw, inventory_identity = bound_json(paths, paths.inventory_path(before_root, receipt), inventory)
    active_file = owner_path() / ".production-active.json"
    active_identity = load("install-rollback").identity(active_file)
    if active_file.exists():
        active_value, _raw, active_identity = bound_json(paths, active_file)
        if active_value.get("root") != str(before_root):
            refused("source_preimage_changed", "active pointer differs from the captured predecessor")
    receipt_sha256 = sha(receipt_raw)
    cohorts = capture_cohorts(staged, owners, observations, descriptor, before_root, receipt_sha256)
    same_volume_destinations(prepared, owners)
    registrations = registration_plan(before_root, staged, prepared)
    same_volume_destinations(prepared, owners, registrations=registrations)
    value = {"schema": SCHEMA, "prepared": prepared, "predecessor": previous,
             "receipt": receipt, "receipt_sha256": receipt_sha256, "receipt_identity": receipt_identity,
             "inventory": inventory, "inventory_sha256": sha(inventory_raw), "inventory_identity": inventory_identity,
             "active_identity": active_identity,
             "registrations": registrations,
             "owners": owners, "tracking": observations, "cohorts": cohorts}
    encoded(value)
    return value


def retain_controller(paths, root, base):
    revision = paths.git(ROOT, "rev-parse", "HEAD")
    if paths.git(ROOT, "status", "--porcelain", "--untracked-files=normal"):
        refused("controller_source_changed", "commit the transition controller before preparing a durable recovery path")
    folder = owner_path() / ".production-transition-controllers" / revision
    paths.protected_directory(folder / "scripts", create=True)
    manifest = {"revision": revision, "base_python": str(paths.durable(base, "transition recovery interpreter")),
                "files": {}}
    for name in CONTROL:
        wanted = (paths.git(ROOT, "show", revision + ":scripts/" + name) + "\n").encode()
        file = folder / "scripts" / name
        if not file.exists():
            fd = os.open(file, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(wanted)
                stream.flush()
                os.fsync(stream.fileno())
        if paths.read_bytes(file) != wanted:
            refused("recovery_controller_changed", "retained controller differs from the exact source Git object")
        manifest["files"][name] = sha(wanted)
    paths.write_json(folder / "manifest.json", manifest)
    paths.sync_tree(folder)
    return [str(base), "-I", "-S", "-B", str(folder / "scripts/production-generation.py")]


def prepare(paths, tag, skills=None, home=None):
    install = load("install-production")
    with stopped_owner(paths) as descriptor:
        before, receipt, _inventory = current(paths)
        paths.production_runtime(before)
        subprocess.run(["git", "check-ref-format", "refs/tags/" + tag], check=True,
                       stdout=subprocess.DEVNULL, env=paths.git_environment())
        generation = uuid.uuid4().hex
        container = owner_path() / ".production-generations" / generation
        paths.protected_directory(container.parent, create=True)
        requests = owner_path() / ".production-preparations"
        paths.protected_directory(requests, create=True)
        intent = {"schema": SCHEMA, "phase": "allocating", "generation_id": generation,
                  "container": str(container), "tag": tag, "predecessor_root": str(before)}
        journal = requests / (generation + ".json")
        paths.write_json(journal, intent)
        container.mkdir(mode=0o700)  # Unknown post-mkdir outcomes are retained, never claimed on replay.
        intent.update(container_identity=paths.node(container), phase="cloning")
        paths.write_json(journal, intent)
        staged = container / "source"
        env = paths.git_environment()
        subprocess.run(["git", "clone", "--no-local", "--no-hardlinks", "--no-checkout", str(before), str(staged)],
                       check=True, env=env, stdout=sys.stderr)
        origin = paths.git(before, "remote", "get-url", "origin")
        subprocess.run(["git", "-C", str(staged), "remote", "set-url", "origin", origin], check=True, env=env)
        subprocess.run(["git", "-C", str(staged), "fetch", "--no-tags", "origin",
                        "refs/tags/" + tag + ":refs/tags/" + tag], check=True, env=env, stdout=sys.stderr)
        revision = paths.git(staged, "rev-parse", "refs/tags/" + tag + "^{commit}")
        subprocess.run(["git", "-C", str(staged), "checkout", "--detach", revision], check=True,
                       env=env, stdout=sys.stderr)
        for name in (*CONTROL, "install-service-entry.py"):
            paths.git(staged, "cat-file", "-e", revision + ":scripts/" + name)
        intent.update(phase="provisioning", root=str(staged), root_identity=paths.node(staged))
        paths.write_json(journal, intent)
        install._OWNER_FD = descriptor
        try:
            install.provision(staged)
        finally:
            install._OWNER_FD = None
        proof = paths._runtime(staged, allow_unpublished=True)
        allocation = {"schema": paths.layout_helper().ALLOCATION_SCHEMA, "state": "provisioned",
                      "generation_id": generation, "root": str(staged), "root_identity": paths.node(staged),
                      "environment_identity": paths.node(staged / "runtime/.venv"),
                      "revision": revision, "tree": proof["tree"]}
        paths.write_json(container / "allocation.json", allocation)
        paths.production_bootstrap(staged, proof["base_python"], create=True)
        proof = paths.generation_runtime(staged)
        install._OWNER_FD = descriptor
        try:
            install.verify_console(paths, staged)
        finally:
            install._OWNER_FD = None
        paths.sync_tree(staged)
        command = retain_controller(paths, staged, Path(proof["base_python"]))
        prepared = {"schema": SCHEMA, "generation_id": generation, "root": str(staged), "proof": proof,
                    "startup": paths.startup_binding(staged),
                    "predecessor_root": str(before), "predecessor_owner_generation": receipt["owner_generation"],
                    "allocation_sha256": sha(paths.read_bytes(container / "allocation.json")),
                    "skills": str(paths.durable(skills or Path(receipt["skills"]), "skill registrations")),
                    "home": str(paths.durable(home or Path(receipt["home"]), "agent registrations")),
                    "recovery_controller": command}
        file = container / "prepared.json"
        paths.write_json(file, prepared)
        intent.update(phase="verified", prepared=str(file), prepared_sha256=sha(paths.read_bytes(file)))
        paths.write_json(journal, intent)
        return {"prepared": str(file), "prepared_sha256": intent["prepared_sha256"]}


def prepared_file(paths, file):
    raw = paths.read_bytes(file)
    value = json.loads(raw)
    root = Path(value["root"])
    paths.layout_helper().prepared(root)
    if file != root.parent / "prepared.json" or value.get("schema") != SCHEMA:
        refused("prepared_namespace_unknown", "prepared evidence left its exact generation namespace")
    return value, sha(raw)


def preflight(paths, file):
    with stopped_owner(paths) as descriptor:
        prepared, prepared_sha = prepared_file(paths, file)
        observed = evidence(paths, prepared, descriptor)
        return {"schema": SCHEMA, "command_id": prepared["generation_id"], "answer": ANSWER,
                "prepared": str(file), "prepared_sha256": prepared_sha,
                "expected_evidence_sha256": sha(encoded(observed))}


def request(paths, file):
    value = paths.read_json(file)
    if (set(value) != {"schema", "command_id", "answer", "prepared", "prepared_sha256", "expected_evidence_sha256"}
            or value["schema"] != SCHEMA or value["answer"] != ANSWER
            or not isinstance(value["command_id"], str) or not re.fullmatch(r"[0-9a-f]{32}", value["command_id"])
            or any(not isinstance(value[name], str) or not re.fullmatch(r"[0-9a-f]{64}", value[name])
                   for name in ("prepared_sha256", "expected_evidence_sha256"))):
        raise ValueError("invalid exact source-successor request")
    prepared, actual = prepared_file(paths, Path(value["prepared"]))
    if prepared["generation_id"] != value["command_id"] or actual != value["prepared_sha256"]:
        refused("prepared_generation_changed", "request does not bind the exact prepared candidate")
    return value, prepared


def extend_snapshot(recovery, backup, names, *, expected=None):
    snapshot = backup / "snapshot.json"
    entries = json.loads(snapshot.read_text())
    for file in names:
        name = str(recovery.canonical_target(file))
        if name not in entries:
            saved = {}
            before = recovery.identity(file, saved=saved)
            saved.update(type=before["type"], before=before)
            if before["type"] == "link":
                saved["target"] = before["target"]
            entries[name] = saved
        if expected is not None and entries[name]["before"] != expected[name]:
            refused("source_preimage_changed", "post-preflight configuration or owner replacement preserved: " + name)
    recovery.save_snapshot(snapshot, entries)


def publish(paths, recovery, backup, file, value, label):
    stage = backup / ("new-" + label)
    paths.write_json(stage, value)
    entries = json.loads((backup / "snapshot.json").read_text())
    saved = entries[str(recovery.canonical_target(file))]
    expected = saved.get("installed", saved["before"])
    if expected["type"] == "absent":
        # The generic legacy helper keeps a staging hardlink until cleanup.
        # Authority readers deliberately require one name, including after a
        # crash. Journal the exact staged inode, then publish with no-replace
        # rename so both ordinary reads and the inverse see one stable identity.
        recovery.remember(backup, file, recovery.identity(stage))
        recovery.sync_object(stage)
        load("install-production").rename_exclusive(stage, file)
        recovery.sync_directories(backup, file.parent)
    else:
        recovery.effect(backup, file, stage)


def apply(paths, file):
    supplied, prepared = request(paths, file)
    owner = owner_path()
    journal = owner / ".production-update.json"
    folder = owner / ".production-transitions" / supplied["command_id"]
    with stopped_owner(paths, recovery=True) as descriptor:
        state = paths.read_json(journal) if journal.exists() else None
        if state and state.get("schema") == SCHEMA and state.get("command") == supplied:
            if state["phase"] == "verified":
                # Reconcile a lost response against the actually active source.
                root, receipt, _inventory = current(paths)
                if (str(root) != prepared["root"] or receipt.get("source_successor") != state["receipt_reference"]
                        or paths.production_runtime(root) != prepared["proof"]):
                    refused("source_activation_unknown", "verified response lacks its exact active acknowledgement")
                return {"phase": "verified", "root": str(root), "receipt": state["receipt_reference"]}
            if state["phase"] != "rolled_back":
                refused("source_transition_unfinished", "run the retained rollback before replaying the same request")
        elif state and state.get("phase") not in TERMINAL:
            refused("source_transition_unfinished", "another unfinished update owns this installation")
        observed = evidence(paths, prepared, descriptor)
        if sha(encoded(observed)) != supplied["expected_evidence_sha256"]:
            refused("source_evidence_changed", "preflight again; the captured owner/cohort/candidate changed")
        before = Path(observed["predecessor"]["runtime"]).parent
        staged = Path(prepared["root"])
        paths.protected_directory(folder, create=True)
        receipt_file = folder / "receipt.json"
        if receipt_file.exists():
            refused("transition_command_used", "this immutable transition command already has retained evidence")
        paths.write_json(folder / "evidence.json", observed)
        retained_request = folder / "request.json"
        paths.write_json(retained_request, supplied)
        state = {"schema": SCHEMA, "phase": "requested", "command": supplied,
                 "evidence": str(folder / "evidence.json"), "before_root": str(before),
                 "after_root": str(staged), "before_receipt": observed["receipt"],
                 "recovery_command": [*prepared["recovery_controller"], "rollback", str(retained_request)]}
        paths.write_json(journal, state)  # Block normal starts before any public registration effect.
        recovery = load("install-rollback")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            recovery.capture(before, Path(prepared["skills"]), Path(prepared["home"]))
        backup = Path(output.getvalue().strip())
        state.update(backup=str(backup), backup_identity=paths.node(backup), phase="captured")
        paths.write_json(journal, state)
        installed = dict(observed["receipt"], owner_generation=prepared["generation_id"],
                         target_identity=paths.node(staged), revision=prepared["proof"]["revision"],
                         proof=prepared["proof"], phase="verified")
        inventory = json.loads(json.dumps(observed["inventory"]))
        inventory.update(owner_generation=installed["owner_generation"], target_identity=installed["target_identity"])
        for item in observed["owners"]:
            records = inventory["configurations"][item["path"]]
            updated = {"config_sha256": sha(json_bytes(item["after"])),
                       "state_root": item["state_root"], "tracking_db": item["tracking_db"]}
            if updated not in records:
                records.append(updated)
        inventory_file = paths.inventory_path(staged, installed)
        birth_file = owner / ".production-generation-births" / (prepared["generation_id"] + ".json")
        paths.protected_directory(inventory_file.parent, create=True)
        paths.protected_directory(birth_file.parent, create=True)
        if os.path.lexists(inventory_file) or os.path.lexists(birth_file):
            refused("generation_birth_occupied", "new generation authority is already occupied; preserve it")
        birth = {**paths.layout_helper().prepared(staged)["allocation"],
                 "schema": paths.layout_helper().GENERATION_SCHEMA, "state": "verified",
                 "startup": prepared["startup"],
                 "predecessor_root": str(before), "transition_command": supplied["command_id"]}
        acknowledgement = {"schema": SCHEMA, "command": supplied,
                           "evidence_sha256": supplied["expected_evidence_sha256"],
                           "before_root": str(before), "after_root": str(staged),
                           "previous": observed["receipt"].get("source_successor"),
                           "owners": observed["owners"], "tracking": observed["tracking"],
                           "helpers": {name: sha((staged / "skills/devflow/scripts" / name).read_bytes())
                                       for name in ("state.py", "schema.sql")},
                           "cohort_sha256": sha(encoded(observed["cohorts"]))}
        paths.write_json(receipt_file, acknowledgement)
        reference = {"path": str(receipt_file), "sha256": sha(paths.read_bytes(receipt_file))}
        installed["source_successor"] = reference
        active = {"schema": paths.layout_helper().ACTIVE_SCHEMA, "generation_id": prepared["generation_id"],
                  "root": str(staged), "birth_sha256": sha(json_bytes(birth)), "transition": reference}
        extra = [Path(name) for name in observed["registrations"]]
        extra += [Path(item["path"]) for item in observed["owners"]]
        extra += [Path(item["before"]["installation_namespace"]["witness"])
                  for item in observed["owners"] if "namespace_witness" in item]
        extra += [inventory_file, birth_file, owner / ".production-install.json", owner / ".production-active.json"]
        expected = dict(observed["registrations"])
        expected.update({str(recovery.canonical_target(Path(item["path"]))): item["before_identity"]
                         for item in observed["owners"]})
        expected.update({str(recovery.canonical_target(Path(item["before"]["installation_namespace"]["witness"]))):
                         item["namespace_witness_identity"] for item in observed["owners"] if "namespace_witness" in item})
        expected.update({str(inventory_file): {"type": "absent"}, str(birth_file): {"type": "absent"},
                         str(owner / ".production-install.json"): observed["receipt_identity"],
                         str(owner / ".production-active.json"): observed["active_identity"]})
        extend_snapshot(recovery, backup, extra, expected=expected)
        # The legacy capture probed only skill/agent targets. Every newly added
        # configuration/witness/authority target must pass the same native
        # exchange boundary before the first registration is changed.
        recovery.preflight(backup, extra)
        state.update(phase="registering", receipt_reference=reference)
        state.update(after_inventory=str(inventory_file), after_inventory_sha256=sha(json_bytes(inventory)))
        paths.write_json(journal, state)
        old_inventory_file = paths.inventory_path(before, observed["receipt"])
        if recovery.identity(old_inventory_file) != observed["inventory_identity"]:
            refused("source_preimage_changed", "configuration census changed before registration effects")
        # All destinations are authenticated and write-ahead captured. An
        # exception leaves a durable unfinished journal; no blind auto-retry.
        entry = load("install-service-entry", staged)
        with contextlib.redirect_stdout(sys.stderr):
            entry.install(Path(prepared["skills"]), Path(prepared["home"]), False, backup,
                          migration_source=before, lease_fd=descriptor, predecessor=before)
        for index, item in enumerate(observed["owners"]):
            publish(paths, recovery, backup, Path(item["path"]), item["after"], "config-" + str(index))
            if "namespace_witness" in item:
                witness = dict(item["namespace_witness"], root=str(staged),
                               owner_generation=installed["owner_generation"],
                               birth_sha256=active["birth_sha256"],
                               config_sha256=sha(json_bytes(item["after"])))
                publish(paths, recovery, backup, Path(item["before"]["installation_namespace"]["witness"]),
                        witness, "namespace-" + str(index))
        publish(paths, recovery, backup, inventory_file, inventory, "inventory")
        publish(paths, recovery, backup, birth_file, birth, "birth")
        if registered_databases(paths, observed["inventory"], observer=before, descriptor=descriptor) != observed["tracking"]:
            refused("tracking_changed", "run state, original authorization or budgets changed during activation")
        if recovery.identity(old_inventory_file) != observed["inventory_identity"]:
            refused("source_preimage_changed", "configuration census changed before source activation")
        publish(paths, recovery, backup, owner / ".production-install.json", installed, "owner")
        publish(paths, recovery, backup, owner / ".production-active.json", active, "active")
        paths.production_runtime(staged)
        state["phase"] = "verified"
        paths.write_json(journal, state)
        # Retain the snapshot even after success for an offline inverse. It
        # must still prove the tracking cohort has not changed before use.
        return {"phase": "verified", "root": str(staged), "receipt": reference}


def json_bytes(value):
    # Match installation-paths.write_json exactly when binding public bytes.
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def restore_snapshot(paths, recovery, backup, *, backup_identity=None):
    """Preserve the original device namespace only under a proved volume birth."""
    original = recovery.identity
    required_device = None
    if backup_identity is not None:
        observed = paths.node(backup)
        if not paths.same_node(backup_identity, observed):
            refused("transition_recovery_custody_unknown", "retained inverse directory changed; preserve all objects")
        required_device = observed["device"]
        if backup_identity["device"] != required_device:
            # same_node permits this only with the original volume UUID,
            # directory inode/birth/owner/mode. All effect targets were proved
            # to share that volume. Never discount a device change by inode
            # alone, and never rewrite the original snapshot/evidence.
            def in_original_volume(path, *args, **kwargs):
                value = original(path, *args, **kwargs)
                if "node" in value:
                    if value["node"][0] != required_device:
                        refused("transition_recovery_filesystem_changed", "inverse object left the authenticated volume")
                    value["node"][0] = backup_identity["device"]
                return value

            recovery.identity = in_original_volume
    try:
        return _restore_snapshot(paths, recovery, backup, required_device=required_device)
    finally:
        recovery.identity = original


def _restore_snapshot(paths, recovery, backup, *, required_device=None):
    """Replay each inverse by its actual published inode, retaining every object.

    The generic legacy restorer retires its backup after a successful return.
    A generation rollover instead keeps a write-ahead per-object inverse so a
    kill between exchange and response cannot turn a completed restore into an
    unknown new object or lose the only offline recovery evidence.
    """
    snapshot = backup / "snapshot.json"
    info = snapshot.lstat()
    if info.st_uid != os.getuid() or info.st_mode & 0o777 != 0o600 or info.st_nlink != 1:
        refused("registration_recovery_unknown", "inverse snapshot lost its private custody")
    captured = {}
    recovery.identity(snapshot, saved=captured)  # Preserve the legacy large-hooks capture contract.
    entries = json.loads(base64.b64decode(captured["bytes"]))
    if required_device is not None:
        for name in entries:
            parent = Path(name).parent
            while not parent.exists():
                parent = parent.parent
            if device(parent) != required_device:
                refused("transition_recovery_filesystem_changed", "inverse destination volume changed: " + name)
    drift = recovery.retained_forward_conflicts(backup, entries)
    if drift or any("drift" in item for item in entries.values()):
        refused("registration_recovery_drift", "retained foreign forward objects require reconciliation")
    journal = backup / "generation-inverse.json"
    inverse = paths.read_json(journal) if journal.exists() else {}
    for name in sorted(entries, key=lambda item: (-len(Path(item).parts), item)):
        saved = entries[name]
        if "installed" not in saved:
            continue
        path = Path(name)
        record = inverse.get(name)
        observed = recovery.identity(path)
        if record and record["phase"] == "restored":
            if observed != record["restored"]:
                refused("registration_recovery_drift", "a restored registration changed: " + name)
            continue
        if record is None:
            if observed == saved["before"]:
                inverse[name] = {"phase": "restored", "restored": observed}
                paths.write_json(journal, inverse)
                continue
            if observed != saved["installed"]:
                refused("registration_recovery_drift", "captured registration changed: " + name)
            kind = saved["type"]
            if kind == "absent" and observed["type"] == "directory":
                # Retain new registration directories. Child entries are
                # independently restored; removing their parent could move a
                # concurrent writer's data during an otherwise valid inverse.
                inverse[name] = {"phase": "restored", "restored": observed, "retained_directory": True}
                paths.write_json(journal, inverse)
                continue
            if kind == "directory":
                refused("registration_recovery_unknown", "a directory replacement is outside inverse custody: " + name)
            stage = backup / ("inverse-" + uuid.uuid4().hex)
            record = {"phase": "prepared", "captured": str(stage), "installed": saved["installed"],
                      "desired_type": kind}
            if kind == "file":
                fd = os.open(stage, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, saved["mode"])
                with os.fdopen(fd, "wb") as stream:
                    stream.write(base64.b64decode(saved["bytes"]))
                    stream.flush()
                    os.fchmod(stream.fileno(), saved["mode"])
                    os.fsync(stream.fileno())
            elif kind == "link":
                stage.symlink_to(saved["target"])
            elif kind != "absent":
                refused("registration_recovery_unknown", "unsupported captured registration kind")
            record["desired"] = recovery.identity(stage)
            recovery.sync_directories(backup)
            inverse[name] = record
            paths.write_json(journal, inverse)  # Intent and staged identity precede the public effect.
        stage = Path(record["captured"])
        desired, installed = record["desired"], record["installed"]
        observed, captured = recovery.identity(path), recovery.identity(stage)
        if observed == desired and captured == installed:
            pass  # Exchange/removal committed before the inverse journal response.
        elif observed == installed and captured == desired:
            if record["desired_type"] == "absent":
                load("install-production").rename_exclusive(path, stage)
            elif installed["type"] == "absent":
                load("install-production").rename_exclusive(stage, path)
            else:
                recovery.exchange(stage, path)
            recovery.sync_directories(backup, path.parent)
            if recovery.identity(path) != desired or recovery.identity(stage) != installed:
                refused("registration_recovery_drift", "inverse captured unknown bytes; preserve both names: " + name)
        else:
            refused("registration_recovery_drift", "inverse effect outcome is unknown; preserve both names: " + name)
        record.update(phase="restored", restored=recovery.identity(path))
        paths.write_json(journal, inverse)


def rollback(paths, file):
    supplied, prepared = request(paths, file)
    journal = owner_path() / ".production-update.json"
    with stopped_owner(paths, recovery=True) as descriptor:
        state = paths.read_json(journal)
        if state.get("schema") != SCHEMA or state.get("command") != supplied:
            refused("transition_recovery_owner_unknown", "this request is not the recorded transition")
        if state["phase"] == "rolled_back":
            return {"phase": "rolled_back", "root": state["before_root"]}
        evidence_file = Path(state["evidence"])
        observed = paths.read_json(evidence_file)
        if sha(encoded(observed)) != supplied["expected_evidence_sha256"]:
            refused("transition_recovery_evidence_changed", "retained evidence differs from the captured request")
        before = Path(state["before_root"])
        if inspect_runtime(paths, before) != observed["predecessor"]:
            refused("transition_predecessor_changed", "retain both generations; previous source/environment changed")
        if registered_databases(paths, observed["inventory"], observer=before, descriptor=descriptor) != observed["tracking"]:
            refused("tracking_changed", "unfinished or newly admitted work must be reconciled before source rollback")
        current_receipt = paths.read_json(owner_path() / ".production-install.json")
        if (current_receipt.get("source_successor") == state.get("receipt_reference")
                and state.get("after_inventory")
                and sha(paths.read_bytes(Path(state["after_inventory"]))) != state["after_inventory_sha256"]):
            refused("owner_enrollment_changed", "newly enrolled stores or configurations must be reconciled before rollback")
        if state.get("backup"):
            backup = Path(state["backup"])
            restore_snapshot(paths, load("install-rollback"), backup, backup_identity=state["backup_identity"])
        else:
            # No public effect is allowed before a captured backup is journaled.
            if paths.read_json(owner_path() / ".production-install.json") != observed["receipt"]:
                refused("transition_recovery_unknown", "owner changed before a complete captured inverse")
        state["phase"] = "rolled_back"
        paths.write_json(journal, state)
        return {"phase": "rolled_back", "root": str(before)}


def allocate_namespace(paths, template):
    """Create a fresh private writer namespace; never enroll an existing store.

    The random namespace and queue did not exist before its write-ahead intent.
    Only the authenticated active generation can bootstrap its state helper;
    ordinary admission is unavailable until the completed witness is durable.
    """
    with stopped_owner(paths) as descriptor:
        root, receipt, _inventory = current(paths)
        layout = paths.layout_helper().installed(root)
        if layout["version"] != 2:
            refused("fresh_namespace_generation_required", "first activate an immutable foundation-born generation")
        folder = Path(layout["birth"]["startup"]["directory"])
        metadata = paths.read_json(folder / "manifest.json")
        source_raw = paths.read_bytes(folder / "production-source.py")
        if (sha(paths.read_bytes(folder / "manifest.json")) != layout["birth"]["startup"]["manifest_sha256"]
                or sha(source_raw) != metadata["source_helper_sha256"]):
            refused("namespace_source_unknown", "qualified allocation source verifier changed")
        spec = importlib.util.spec_from_file_location("allocation_source", folder / "production-source.py")
        source = importlib.util.module_from_spec(spec)
        exec(compile(source_raw, str(folder / "production-source.py"), "exec", dont_inherit=True), source.__dict__)
        source.check_image(root)  # Before the first SDK/bootstrap child imports.
        paths.production_runtime(root)
        raw = paths.read_bytes(template)
        before = json.loads(raw)
        paths.configuration(template)
        namespace_id = uuid.uuid4().hex
        parent = paths.persistence_path(Path.home().resolve() / ".local/state/devflow/namespaces",
                                        "fresh namespace parent")
        paths.protected_directory(parent, create=True)
        # Allocation intent lives outside the directory whose mkdir result may
        # become unknown. A replay never claims a preexisting namespace.
        intents = owner_path() / ".production-namespace-allocations"
        paths.protected_directory(intents, create=True)
        intent_file = intents / (namespace_id + ".json")
        folder = parent / namespace_id
        intent = {"schema": "devflow.fresh-namespace/v1", "phase": "allocating",
                  "namespace_id": namespace_id, "root": str(root),
                  "owner_generation": receipt["owner_generation"], "birth_sha256": layout["birth_sha256"],
                  "namespace": str(folder), "template": str(template), "template_sha256": sha(raw)}
        paths.write_json(intent_file, intent)
        folder.mkdir(mode=0o700)
        state = folder / "state"
        state.mkdir(mode=0o700)
        config = folder / "config.json"
        database_file = folder / "tracking.sqlite3"
        value = dict(before, state_root=str(state), tracking_db=str(database_file),
                     helpers_dir=str(root / "skills/devflow/scripts"),
                     queue=before.get("queue", "devflow-delivery") + "-" + namespace_id,
                     installation_namespace={"version": 1, "namespace_id": namespace_id,
                                             "witness": str(folder / "witness.json")})
        if value.get("provider", "codex") == "codex":
            binaries = list((root / "runtime/.venv/lib").glob("python*/site-packages/codex_cli_bin/bin/codex"))
            if len(binaries) != 1:
                refused("bundled_cli_unknown", "namespace requires the active generation's measured bundled CLI")
            value["codex_bin"] = str(binaries[0])
        paths.write_json(config, value)
        intent.update(phase="bootstrapping", namespace_identity=paths.node(folder), state_identity=paths.node(state),
                      config=str(config), config_sha256=sha(paths.read_bytes(config)), database=str(database_file))
        paths.write_json(folder / "allocation.json", intent)
        paths.write_json(intent_file, intent)
        program = ("import json,pathlib,sys;from devflow_temporal.delivery_config import DeliveryConfig;"
                   "from devflow_temporal.delivery_store import DeliveryStore;"
                   "p=pathlib.Path(sys.argv[1]);c=DeliveryConfig.load(p);"
                   "DeliveryStore(c)")
        subprocess.run([str(root / "runtime/.venv/bin/python"), "-I", "-B", "-X", "pycache_prefix=/dev/null", "-c", program, str(config)],
                       check=True, timeout=90, pass_fds=(descriptor,),
                       env={**os.environ, "DEVFLOW_NAMESPACE_ALLOCATION_FD": str(descriptor)})
        observed = observed_database(root, database_file, descriptor)
        if observed["runs"]:
            refused("namespace_bootstrap_changed", "fresh namespace already contains admitted work")
        # Reuse the actual runtime's opened-file identity, not a guessed inode
        # or a caller declaration that a previously existing store was empty.
        identity_program = ("import json,sys;from devflow_temporal.delivery_installation_owner import "
                            "namespace_database_identity;print(json.dumps(namespace_database_identity(sys.argv[1])))")
        actual = subprocess.check_output([str(root / "runtime/.venv/bin/python"), "-I", "-B", "-c",
                                          identity_program, str(database_file)], timeout=30)
        witness = dict(intent, phase="verified", database_identity=json.loads(actual),
                       bootstrap_tracking_sha256=observed["sha256"])
        paths.write_json(folder / "witness.json", witness)
        intent.update(phase="verified", witness=str(folder / "witness.json"),
                      witness_sha256=sha(paths.read_bytes(folder / "witness.json")))
        paths.write_json(intent_file, intent)
        paths.write_json(folder / "allocation.json", intent)
        return {"phase": "verified", "config": str(config), "namespace_id": namespace_id,
                "witness": intent["witness"], "witness_sha256": intent["witness_sha256"]}


def main():
    paths = load("installation-paths")
    args = sys.argv[1:]
    if args and args[0] == "prepare" and 2 <= len(args) <= 4:
        return prepare(paths, args[1], *(Path(value).absolute() for value in args[2:]))
    if len(args) == 2 and args[0] in {"preflight", "apply", "rollback"}:
        return globals()[args[0]](paths, Path(args[1]).absolute())
    if len(args) == 2 and args[0] == "allocate-namespace":
        return allocate_namespace(paths, Path(args[1]).absolute())
    raise ValueError("usage: production-generation.py prepare TAG [SKILLS HOME] | preflight PREPARED | apply REQUEST | rollback REQUEST")


if __name__ == "__main__":
    previous_umask = os.umask(0o077)
    try:
        print(json.dumps(main(), sort_keys=True, indent=2))
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        print(json.dumps({"phase": "unknown", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        raise SystemExit(1) from exc
    finally:
        os.umask(previous_umask)
