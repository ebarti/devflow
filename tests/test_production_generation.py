"""Owned filesystem/SQLite fault probes for immutable source rollover."""

import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import test_production_installation as installation_fixtures

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class SourceGenerationProtocol(unittest.TestCase):
    def setUp(self):
        self.generation = module("production-generation")
        self.paths = module("installation-paths")
        self.recovery = module("install-rollback")
        self.temporary = tempfile.TemporaryDirectory(prefix="devflow-generation-small-")
        self.addCleanup(self.temporary.cleanup)
        self.home = Path(self.temporary.name).resolve()
        self.backup = self.home / "backup"
        self.backup.mkdir(mode=0o700)
        self.target = self.home / "registration"

    def private(self, path, value):
        path.write_text(json.dumps(value))
        path.chmod(0o600)

    def reader(self):
        # Only the OS-temp lexical policy is adapted, never I/O or effects.
        return patch.object(self.paths, "durable", side_effect=lambda value, *args, **kwargs: Path(value))

    def snapshot(self, before=b"original", after=b"installed"):
        self.target.write_bytes(before)
        self.target.chmod(0o600)
        saved = {}
        original = self.recovery.identity(self.target, saved=saved)
        saved.update(type=original["type"], before=original)
        self.private(self.backup / "snapshot.json", {str(self.target): saved})
        stage = self.backup / "forward"
        stage.write_bytes(after)
        stage.chmod(0o600)
        self.recovery.effect(self.backup, self.target, stage)

    def test_inverse_survives_real_exchange_before_response_and_keeps_offline_objects(self):
        self.snapshot()
        code = '''
import importlib.util,os,pathlib,sys
def load(name):
 s=importlib.util.spec_from_file_location(name,pathlib.Path(sys.argv[1])/'scripts'/(name+'.py'))
 m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
g,p,r=map(load,('production-generation','installation-paths','install-rollback'))
p.durable=lambda value,*args,**kwargs:pathlib.Path(value)
real=r.exchange
def crash(left,right):
 real(left,right);os._exit(77)
r.exchange=crash
g.restore_snapshot(p,r,pathlib.Path(sys.argv[2]))
'''
        killed = subprocess.run([sys.executable, "-I", "-B", "-c", code, str(ROOT), str(self.backup)],
                                capture_output=True, text=True)
        self.assertEqual(killed.returncode, 77, killed.stderr)
        self.assertEqual(self.target.read_bytes(), b"original")
        with self.reader():
            self.generation.restore_snapshot(self.paths, self.recovery, self.backup)
            self.generation.restore_snapshot(self.paths, self.recovery, self.backup)
        self.assertTrue((self.backup / "generation-inverse.json").exists())
        self.assertIn(b"installed", [file.read_bytes() for file in self.backup.glob("inverse-*")])

    def test_inverse_refuses_replacement_after_capture_and_preserves_every_byte(self):
        self.snapshot()
        replacement = self.home / "foreign"
        replacement.write_bytes(b"new publisher bytes")
        replacement.replace(self.target)
        with self.reader(), self.assertRaisesRegex(ValueError, "registration_recovery_drift"):
            self.generation.restore_snapshot(self.paths, self.recovery, self.backup)
        self.assertEqual(self.target.read_bytes(), b"new publisher bytes")
        self.assertTrue((self.backup / "snapshot.json").exists())

    def test_post_evidence_config_replacement_is_refused_before_snapshot_or_effect(self):
        self.private(self.target, {"authorization": "owned", "remaining": 7})
        self.private(self.backup / "snapshot.json", {})
        with self.reader():
            _value, raw, expected = self.generation.bound_json(self.paths, self.target)
        self.assertIn(b'"remaining": 7', raw)
        replacement = self.home / "new-config"
        self.private(replacement, {"authorization": "new operator edit", "remaining": 11})
        replacement.replace(self.target)  # Actual replace after evidence, before capture.
        current = self.target.read_bytes()
        with self.assertRaisesRegex(ValueError, "source_preimage_changed"):
            self.generation.extend_snapshot(self.recovery, self.backup, [self.target],
                                             expected={str(self.target): expected})
        self.assertEqual(self.target.read_bytes(), current)
        self.assertEqual(json.loads((self.backup / "snapshot.json").read_text()), {})

    def test_complete_extra_destination_preflight_refuses_owned_cross_device_before_effects(self):
        # Native temp RAM mounts are not created by this test. /dev on Darwin
        # and Linux is a distinct existing filesystem, observed only by stat.
        config = self.home / "config.json"
        self.private(config, {"remaining": 7})
        old = config.read_bytes()
        prepared = {"home": str(self.home), "skills": str(self.home / "skills")}
        with patch.object(self.generation, "owner_path", return_value=self.home):
            self.generation.same_volume_destinations(prepared, [{"path": str(config)}])
            if Path("/dev").stat().st_dev != self.home.stat().st_dev:
                with self.assertRaisesRegex(ValueError, "transition_filesystem_unsupported"):
                    self.generation.same_volume_destinations(prepared, [{"path": "/dev/not-created-devflow-config"}])
                with self.assertRaisesRegex(ValueError, "transition_filesystem_unsupported"):
                    self.generation.same_volume_destinations({**prepared, "skills": "/dev/devflow-skills-not-created"},
                                                            [{"path": str(config)}])
                with self.assertRaisesRegex(ValueError, "transition_filesystem_unsupported"):
                    self.generation.same_volume_destinations(prepared, [{"path": str(config)}],
                                                            registrations=["/dev/devflow-agent-not-created"])
        self.assertEqual(config.read_bytes(), old)
        self.assertFalse(Path("/dev/not-created-devflow-config").exists())
        self.assertFalse(Path("/dev/devflow-skills-not-created").exists())

    def test_agent_rollover_requires_exact_predecessor_and_preserves_custom_copies(self):
        import hashlib

        agents = module("install-agents")
        before, after = self.home / "prior", self.home / "next"
        for root in (before, after):
            (root / "agents").mkdir(parents=True, mode=0o700)
        old = before / "agents/devflow-owned.toml"
        new = after / "agents/devflow-owned.toml"
        old.write_bytes(b"old owned source")
        new.write_bytes(b"new owned source")
        codex = self.home / "codex"
        installed = codex / "agents"
        installed.mkdir(parents=True, mode=0o700)
        copy = installed / old.name
        copy.write_bytes(old.read_bytes())
        manifest = {"schema_version": 1, "source_root": str(before), "agents": {
            old.name: {"source": str(old), "sha256": hashlib.sha256(old.read_bytes()).hexdigest()}}}
        self.private(installed / ".devflow-agent-manifest.json", manifest)
        skills = self.home / "skills"
        skills.mkdir(mode=0o700)
        with self.assertRaisesRegex(ValueError, "another checkout"):
            agents.plan(after, skills, codex, False)
        self.assertEqual(agents.plan(after, skills, codex, False, predecessor=before)[2], {old.name: "copy"})
        with self.assertRaisesRegex(ValueError, "exact acknowledged source"):
            agents.plan(after, skills, codex, False, predecessor=self.home / "unrelated")
        copy.write_bytes(b"operator customization")
        with self.assertRaisesRegex(ValueError, "Modified or custom"):
            agents.plan(after, skills, codex, False, predecessor=before)
        self.assertEqual(copy.read_bytes(), b"operator customization")

    def test_inverse_uses_actual_volume_birth_for_device_renumber_adapter(self):
        self.snapshot()
        birth = self.paths.node(self.backup)
        if not birth.get("volume_uuid") or birth.get("birthtime") is None:
            self.skipTest("host does not supply an actual stable volume/birth identity")
        changed_device = birth["device"] + 1
        original_identity = self.recovery.identity
        original_node = self.paths.node

        def renumbered_identity(path, *args, **kwargs):
            value = original_identity(path, *args, **kwargs)
            if "node" in value:
                self.assertEqual(value["node"][0], birth["device"])
                value["node"][0] = changed_device
            return value

        def renumbered_node(path):
            value = original_node(path)
            self.assertEqual(value["device"], birth["device"])
            return {**value, "device": changed_device}

        # Only the device-number observation changes. Actual APFS UUID/birth,
        # inodes, opened bytes and native filesystem exchanges remain real.
        with self.reader(), patch.object(self.paths, "node", side_effect=renumbered_node), \
                patch.object(self.recovery, "identity", side_effect=renumbered_identity), \
                patch.object(self.generation, "device", return_value=changed_device):
            self.generation.restore_snapshot(self.paths, self.recovery, self.backup, backup_identity=birth)
            self.generation.restore_snapshot(self.paths, self.recovery, self.backup, backup_identity=birth)
        self.assertEqual(self.target.read_bytes(), b"original")
        foreign_birth = {**birth, "birthtime": birth["birthtime"] + 1}
        with self.reader(), self.assertRaisesRegex(ValueError, "transition_recovery_custody_unknown"):
            self.generation.restore_snapshot(self.paths, self.recovery, self.backup, backup_identity=foreign_birth)
        self.assertEqual(self.target.read_bytes(), b"original")

    def test_fake_empty_policy_readiness_role_grant_still_requires_host_body_closure(self):
        from temporalio.api.history.v1 import HistoryEvent
        from temporalio.client import WorkflowHistory
        from devflow_temporal.delivery_runtime_successor import (
            TransitionRefused, require_closed_body_custody,
        )
        from devflow_temporal.delivery_plan_checks import planned_checks

        # Fake role readiness can derive `uv run ...` from this accepted plan
        # even though no configured check exists. SDK start grants the body;
        # a completed waiter/result cannot prove its subprocesses settled.
        original = {"provider": "fake", "readiness_version": 1,
                    "accepted_plan": json.dumps({"verification": ["Run test_owned.py"]}),
                    "policy": {"checks": [], "prepublish_checks": [], "browser_qa": None}}
        self.assertEqual(original["provider"], "fake")
        checkout = self.home / "locked-project"
        checkout.mkdir(mode=0o700)
        (checkout / "test_owned.py").write_text("def test_owned(): assert True\n")
        (checkout / "pyproject.toml").write_text('[project]\nname="fixture"\nversion="1"\n')
        (checkout / "uv.lock").write_text("version=1\n")
        subprocess.run(["git", "init", "-q", str(checkout)], check=True)
        subprocess.run(["git", "-C", str(checkout), "add", "."], check=True)
        recipes = planned_checks(original, checkout, self.home / "evidence", preparation=True)
        dependencies = [item for item in recipes if item["id"].startswith("planned-python-dependencies-")]
        self.assertEqual(len(dependencies), 1)
        self.assertEqual(dependencies[0]["argv"][1:3], ["sync", "--locked"])
        scheduled = HistoryEvent(event_id=1, activity_task_scheduled_event_attributes={
            "activity_type": {"name": "delivery_role"}})
        started = HistoryEvent(event_id=2, activity_task_started_event_attributes={"scheduled_event_id": 1})
        completed = HistoryEvent(event_id=3, activity_task_completed_event_attributes={
            "scheduled_event_id": 1, "started_event_id": 2})
        history = WorkflowHistory("owned-fake-readiness", [scheduled, started, completed])
        with self.assertRaises(TransitionRefused) as refused:
            require_closed_body_custody(history)
        self.assertIn("host_body_custody_unknown", json.dumps(refused.exception.assessment))
        # A schedule with no start grants no body. Pure actual projections
        # remain the useful supported closed cohort instead of provider labels.
        self.assertEqual(require_closed_body_custody(WorkflowHistory("not-started", [scheduled])), [])
        projection = HistoryEvent(event_id=1, activity_task_scheduled_event_attributes={
            "activity_type": {"name": "delivery_project"}})
        self.assertEqual(require_closed_body_custody(WorkflowHistory("metadata-only", [projection, started, completed])), [])

    def test_inverse_keeps_created_directories_and_restores_known_file(self):
        self.snapshot()
        directory = self.home / "created"
        directory.mkdir(mode=0o700)
        foreign = directory / "retained"
        foreign.write_bytes(b"owner data")
        snapshot = json.loads((self.backup / "snapshot.json").read_text())
        snapshot[str(directory)] = {"type": "absent", "before": {"type": "absent"},
                                    "installed": self.recovery.identity(directory)}
        self.private(self.backup / "snapshot.json", snapshot)
        with self.reader():
            self.generation.restore_snapshot(self.paths, self.recovery, self.backup)
        self.assertEqual(foreign.read_bytes(), b"owner data")
        self.assertEqual(self.target.read_bytes(), b"original")

    def test_new_authority_publication_has_one_name_and_recoverable_inverse(self):
        self.private(self.backup / "snapshot.json", {str(self.target): {
            "type": "absent", "before": {"type": "absent"}}})
        with self.reader():
            self.generation.publish(self.paths, self.recovery, self.backup, self.target,
                                    {"phase": "verified", "authority": "owned"}, "authority")
            self.assertEqual(json.loads(self.paths.read_bytes(self.target)),
                             {"phase": "verified", "authority": "owned"})
            self.assertEqual(self.target.stat().st_nlink, 1)
            self.generation.restore_snapshot(self.paths, self.recovery, self.backup)
        self.assertFalse(self.target.exists())
        self.assertTrue((self.backup / "generation-inverse.json").exists())

    def test_authority_publication_reconciles_real_death_on_both_sides_of_rename(self):
        code = '''
import importlib.util,os,pathlib,sys
def load(name):
 s=importlib.util.spec_from_file_location(name,pathlib.Path(sys.argv[1])/'scripts'/(name+'.py'))
 m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
g,p,r=map(load,('production-generation','installation-paths','install-rollback'))
p.durable=lambda value,*args,**kwargs:pathlib.Path(value)
real_load=g.load
def intercepted(name,*args,**kwargs):
 m=real_load(name,*args,**kwargs)
 if name=='install-production':
  rename=m.rename_exclusive
  def boundary(source,target):
   if sys.argv[4]=='before':os._exit(77)
   rename(source,target);os._exit(77)
  m.rename_exclusive=boundary
 return m
g.load=intercepted
g.publish(p,r,pathlib.Path(sys.argv[2]),pathlib.Path(sys.argv[3]),{'phase':'verified'},'authority')
'''
        for boundary in ("before", "after"):
            with self.subTest(boundary=boundary):
                backup = self.home / ("backup-" + boundary)
                backup.mkdir(mode=0o700)
                target = self.home / ("authority-" + boundary)
                self.private(backup / "snapshot.json", {str(target): {
                    "type": "absent", "before": {"type": "absent"}}})
                killed = subprocess.run([sys.executable, "-I", "-B", "-c", code,
                                         str(ROOT), str(backup), str(target), boundary],
                                        capture_output=True, text=True)
                self.assertEqual(killed.returncode, 77, killed.stderr)
                self.assertEqual(target.exists(), boundary == "after")
                if target.exists():
                    self.assertEqual(target.stat().st_nlink, 1)
                with self.reader():
                    self.generation.restore_snapshot(self.paths, self.recovery, backup)
                    self.generation.restore_snapshot(self.paths, self.recovery, backup)
                self.assertFalse(target.exists())
                self.assertTrue((backup / "snapshot.json").exists())

    def test_competing_publication_at_no_replace_boundary_keeps_foreign_destination(self):
        self.private(self.backup / "snapshot.json", {str(self.target): {
            "type": "absent", "before": {"type": "absent"}}})
        real_load = self.generation.load

        def competing(name, *args, **kwargs):
            result = real_load(name, *args, **kwargs)
            if name == "install-production":
                real_rename = result.rename_exclusive

                def race(source, destination):
                    destination.write_bytes(b"concurrent registration")
                    destination.chmod(0o600)
                    return real_rename(source, destination)

                result.rename_exclusive = race
            return result

        with self.reader(), patch.object(self.generation, "load", side_effect=competing):
            with self.assertRaises(FileExistsError):
                self.generation.publish(self.paths, self.recovery, self.backup, self.target,
                                        {"phase": "verified"}, "authority")
        self.assertEqual(self.target.read_bytes(), b"concurrent registration")
        with self.reader(), self.assertRaisesRegex(ValueError, "registration_recovery_drift"):
            self.generation.restore_snapshot(self.paths, self.recovery, self.backup)
        self.assertEqual(self.target.read_bytes(), b"concurrent registration")
        self.assertTrue((self.backup / "new-authority").exists())

    def test_complete_snapshot_detects_authorization_budget_and_outbox_changes(self):
        database = self.home / "tracking.db"
        with sqlite3.connect(database) as db:
            db.execute("CREATE TABLE delivery_runs(run_id TEXT PRIMARY KEY,request_json TEXT)")
            db.execute("CREATE TABLE delivery_outbox(run_id TEXT,payload BLOB)")
            db.execute("INSERT INTO delivery_runs VALUES (?,?)", ("owned", '{"budget":7,"grant":"fixture"}'))
            db.execute("INSERT INTO delivery_outbox VALUES (?,?)", ("owned", b"requested"))
        first = self.generation.database(database)
        with sqlite3.connect(database) as db:
            db.execute("UPDATE delivery_runs SET request_json=?", ('{"budget":6,"grant":"fixture"}',))
        self.assertNotEqual(self.generation.database(database)["sha256"], first["sha256"])
        with sqlite3.connect(database) as db:
            db.execute("UPDATE delivery_runs SET request_json=?", ('{"budget":7,"grant":"fixture"}',))
            db.execute("UPDATE delivery_outbox SET payload=?", (b"running",))
        self.assertNotEqual(self.generation.database(database)["sha256"], first["sha256"])

    def test_missing_store_with_possible_writer_stays_unknown(self):
        missing = str(self.home / "tracking.db")
        inventory = {"configurations": {}, "store_observations": {
            missing: {"first_observed_absent": True, "state_roots": []}}, "store_births": {}}
        with patch.object(self.paths, "persistence_path", side_effect=lambda value, *args: Path(value)):
            self.assertEqual(self.generation.registered_databases(self.paths, inventory),
                             {missing: {"absent_before_any_writer": True}})
            inventory["store_births"][missing] = {"intent_version": 1, "state_roots": []}
            with self.assertRaisesRegex(ValueError, "tracking_store_unavailable"):
                self.generation.registered_databases(self.paths, inventory)

    def test_observation_uses_actual_readonly_snapshot_and_never_creates_missing_shm(self):
        database = self.home / "observed.sqlite3"
        db = sqlite3.connect(database)
        self.addCleanup(db.close)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE delivery_runs(run_id TEXT,request_json TEXT)")
        db.commit()
        database.chmod(0o600)
        # Existing runtime assets only; no environment provisioning/downloads.
        runtime = Path(sys.prefix).parent.parent
        descriptor = os.open(self.home / "owner.lock", os.O_CREAT | os.O_RDWR, 0o600)
        self.addCleanup(os.close, descriptor)
        expected = self.generation.database(database)
        actual = self.generation.observed_database(runtime, database, descriptor)
        self.assertEqual(actual, expected)
        shm = Path(str(database) + "-shm")
        self.assertTrue(shm.exists())
        shm.unlink()  # Only this fixture's mapped sidecar is removed.
        before = database.read_bytes()
        with self.assertRaisesRegex(ValueError, "tracking_snapshot_unavailable"):
            self.generation.observed_database(runtime, database, descriptor)
        self.assertEqual(database.read_bytes(), before)
        self.assertFalse(shm.exists(), "read-only preflight recreated a source sidecar")

    def test_direct_namespace_helper_from_old_sdk_refuses_before_schema_creation(self):
        database = self.home / ".local/state/devflow/namespaces" / ("a" * 32) / "tracking.sqlite3"
        database.parent.mkdir(parents=True, mode=0o700)
        program = ("import importlib.util,sys;from pathlib import Path;sys.path.insert(0,sys.argv[1]);"
                   "s=importlib.util.spec_from_file_location('state',sys.argv[2]);"
                   "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);m.connect(Path(sys.argv[3]))")
        result = subprocess.run([sys.executable, "-I", "-B", "-c", program, str(ROOT / "runtime/src"),
                                 str(ROOT / "skills/devflow/scripts/state.py"), str(database)],
                                env={**os.environ, "HOME": str(self.home)}, capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("active generation SDK and interpreter", result.stderr)
        self.assertFalse(database.exists())

    def test_namespace_scope_refuses_second_owner_before_observing_any_store(self):
        from types import SimpleNamespace
        from devflow_temporal import delivery_installation_owner as owner

        folder = self.home / ".local/state/devflow/namespaces" / ("e" * 32)
        folder.mkdir(parents=True, mode=0o700)
        state = folder / "state"
        state.mkdir(mode=0o700)
        database = folder / "tracking.sqlite3"
        database.touch(mode=0o600)
        config = folder / "config.json"
        self.private(config, {"policy": "original"})
        for path, state_root in ((self.home / "another.json", state), (config, folder / "aliased-state")):
            supplied = SimpleNamespace(path=path, state_root=state_root, tracking_db=database)
            with patch.dict(os.environ, HOME=str(self.home)), \
                    patch.object(owner, "require_namespace_database", side_effect=AssertionError("must reject caller first")):
                with self.assertRaisesRegex(ValueError, "exact allocated config, state and tracking"):
                    with owner.fresh_namespace_scope(supplied):
                        self.fail("unrelated namespace owner was admitted")
        self.assertEqual(database.read_bytes(), b"")

    def test_bootstrap_classifies_only_disclosed_namespace_configs_and_owner_references(self):
        entry = module("production-entry")
        ordinary, forwarded = self.home / "ordinary.json", self.home / "forwarded.json"
        namespace = self.home / ".local/state/devflow/namespaces" / ("f" * 32)
        namespace.mkdir(parents=True, mode=0o700)
        config = namespace / "config.json"
        self.private(ordinary, {"tracking_db": str(self.home / "ordinary.db")})
        self.private(config, {"tracking_db": str(namespace / "tracking.sqlite3")})
        self.private(forwarded, {"owners": [str(ordinary), str(config)]})
        forwarded.chmod(0o644)  # Supported Project synchronizer configuration mode.
        with patch.dict(os.environ, HOME=str(self.home)):
            self.assertFalse(entry.namespace_requested(["--help"]))
            self.assertFalse(entry.namespace_requested(["--config", str(ordinary), "status"]))
            self.assertTrue(entry.namespace_requested(["--config=" + str(config), "--help"]))
            self.assertTrue(entry.namespace_requested(["--conf", str(config), "--help"]))
            self.assertTrue(entry.namespace_requested(["--config", "~/" + str(config.relative_to(self.home)), "--help"]))
            self.assertTrue(entry.namespace_requested(["--config", str(forwarded)]))
            forwarded.write_text(json.dumps({"owners": [str(config)], "retained": "x" * (4 * 1024 * 1024)}))
            self.assertTrue(entry.namespace_requested(["--config", str(forwarded)]))
            actual_pread = os.pread

            def changed(descriptor, count, offset):
                raw = actual_pread(descriptor, count, offset)
                forwarded.write_text(json.dumps({"owners": [str(config)], "changed": True}))
                return raw

            with patch.object(entry.os, "pread", side_effect=changed):
                with self.assertRaisesRegex(ValueError, "configuration changed"):
                    entry.namespace_requested(["--config", str(forwarded)])

            def removed(descriptor, count, offset):
                raw = actual_pread(descriptor, count, offset)
                forwarded.unlink()
                return raw

            with patch.object(entry.os, "pread", side_effect=removed):
                with self.assertRaisesRegex(ValueError, "configuration changed"):
                    entry.namespace_requested(["--config", str(forwarded)])

    def test_direct_namespace_helper_refuses_symlink_and_alternate_name_without_effects(self):
        folder = self.home / ".local/state/devflow/namespaces" / ("b" * 32)
        folder.mkdir(parents=True, mode=0o700)
        external = self.home / "sentinel.sqlite3"
        with sqlite3.connect(external) as db:
            db.execute("CREATE TABLE sentinel(value TEXT)")
            db.execute("INSERT INTO sentinel VALUES ('preserved')")
        before = external.read_bytes()
        linked = folder / "tracking.sqlite3"
        linked.symlink_to(external)
        alternate = folder / "other.sqlite3"
        program = ("import importlib.util,sys;from pathlib import Path;sys.path.insert(0,sys.argv[1]);"
                   "s=importlib.util.spec_from_file_location('state',sys.argv[2]);"
                   "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);m.connect(Path(sys.argv[3]))")
        for target in (linked, alternate):
            with self.subTest(target=target.name):
                result = subprocess.run([sys.executable, "-I", "-B", "-c", program, str(ROOT / "runtime/src"),
                                         str(ROOT / "skills/devflow/scripts/state.py"), str(target)],
                                        env={**os.environ, "HOME": str(self.home)}, capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("canonical enrolled database without aliases", result.stderr)
        self.assertTrue(linked.is_symlink())
        self.assertEqual(external.read_bytes(), before)
        self.assertFalse(alternate.exists())
        self.assertEqual(sorted(file.name for file in folder.iterdir()), ["tracking.sqlite3"])


@unittest.skipUnless(os.environ.get("DEVFLOW_PRODUCTION_FIXTURE_PARENT"),
                     "requires an owned durable provisioning parent and the shared disk/probe budget")
class GenerationFixture(unittest.TestCase):
    EMPTY_GENERATION_FIXTURE = True

    @classmethod
    def setUpClass(cls):
        installation_fixtures.ProductionInstallation.setUpClass.__func__(cls)

    def command(self, *args, root=None, check=True, env=None):
        result = subprocess.run(["sh", str((root or self.target) / "scripts/update.sh"), *map(str, args)],
                                env=env or self.env, capture_output=True, text=True, timeout=180)
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + "\n" + result.stderr)
            return json.loads(result.stdout)
        return result

    def managed_python(self, root, program, *arguments):
        # Exercise the same private retained bootstrap as managed API/worker
        # children. This is not a public general-purpose SDK execution command.
        birth = json.loads((self.target.parent / ".production-generation-births" /
                            (root.parent.name + ".json")).read_text())
        folder = Path(birth["startup"]["directory"])
        metadata = json.loads((folder / "manifest.json").read_text())
        return subprocess.run([metadata["base_python"], "-I", "-S", "-B",
                               str(folder / "production-entry.py"), str(root), "--python", "-c", program, *arguments],
                              env=self.env, capture_output=True, text=True, timeout=90)

    def prepare_and_apply(self):
        self.assertFalse(self.origin.exists())
        self.assertFalse(self.setup_python.exists())
        self.assertEqual(self.source_provenance["root_head"], self.source_provenance["fixture_head"])
        self.assertEqual(self.source_provenance["tracked_input_diff_sha256"],
                         __import__("hashlib").sha256(b"").hexdigest(), "fixture must use a frozen committed source")
        subprocess.run(["git", "-C", str(self.target), "tag", "owned-source-generation"], check=True)
        subprocess.run(["git", "-C", str(self.target), "push", "origin", "owned-source-generation"],
                       check=True, capture_output=True)
        result = self.command("--prepare", "owned-source-generation")
        prepared = Path(result["prepared"])
        # Exercise the actual public read-only preflight against two foreign
        # registrations with otherwise valid source, environment and agents.
        receipt = self.target.parent / ".production-install.json"
        before_receipt = receipt.read_bytes()
        journal = self.target.parent / ".production-update.json"
        before_journal = journal.read_bytes() if journal.exists() else None
        for registration in (self.skills / "devflow-local-delivery", self.skills / "devflow/scripts"):
            with self.subTest(foreign_registration=registration.name):
                original = os.readlink(registration)
                foreign = self.home / ("foreign-" + registration.name)
                foreign.mkdir(mode=0o700)
                (foreign / "state.py").write_bytes(b"foreign bytes")
                (foreign / "schema.sql").write_bytes(b"foreign bytes")
                registration.unlink()
                registration.symlink_to(foreign)
                refused = self.command("--preflight", prepared, check=False)
                self.assertNotEqual(refused.returncode, 0)
                self.assertTrue("foreign service entry preserved" in refused.stderr
                                or "Retained helper preserved" in refused.stderr, refused.stderr)
                self.assertEqual(registration.resolve(), foreign)
                self.assertEqual(receipt.read_bytes(), before_receipt)
                self.assertEqual(journal.read_bytes() if journal.exists() else None, before_journal)
                registration.unlink()
                registration.symlink_to(original)
        before = {str(file.relative_to(self.home)): (file.stat().st_ino, file.stat().st_mtime_ns,
                                                    file.read_bytes())
                  for file in self.home.rglob("*") if file.is_file() and not file.is_symlink()
                  and (file.is_relative_to(self.home / "owned-state")
                       or file.is_relative_to(self.target.parent / ".production-configurations")
                       or file == self.config)}
        captured = self.command("--preflight", prepared)
        after = {str(file.relative_to(self.home)): (file.stat().st_ino, file.stat().st_mtime_ns,
                                                   file.read_bytes())
                 for file in self.home.rglob("*") if file.is_file() and not file.is_symlink()
                 and (file.is_relative_to(self.home / "owned-state")
                      or file.is_relative_to(self.target.parent / ".production-configurations")
                      or file == self.config)}
        self.assertEqual(after, before, "preflight wrote to owner inventory/config/tracking state")
        request = self.home / "captured-request.json"
        request.write_text(json.dumps(captured))
        request.chmod(0o600)
        applied = self.command("--apply", request)
        root = Path(applied["root"])
        self.assertTrue(root.is_relative_to(self.target.parent / ".production-generations"))
        self.assertEqual(self.command("--apply", request), applied, "lost-response replay changed the outcome")
        old = subprocess.run([str(self.target / "runtime/.venv/bin/devflow-delivery"), "--help"],
                             env=self.env, capture_output=True, text=True)
        self.assertNotEqual(old.returncode, 0, "previous source retained operational ownership")
        for name in ("devflow-delivery", "devflow-delivery-mcp"):
            direct = subprocess.run([str(root / "runtime/.venv/bin" / name), "--help"],
                                    env=self.env, capture_output=True, text=True)
            self.assertEqual(direct.returncode, 0, direct.stderr)
            self.assertIn("usage:", direct.stdout.lower())
        return request, root


class GenerationRollbackInstallation(GenerationFixture):
    def test_public_generation_rollback_uses_retained_controller_without_uv(self):
        request, root = self.prepare_and_apply()
        state = json.loads((self.target.parent / ".production-update.json").read_text())
        request.unlink()  # The recorded inverse must use its independent retained request.
        tools = self.home / "offline-tools"
        tools.mkdir(mode=0o700)
        uv = tools / "uv"
        uv.write_text("#!/bin/sh\nexit 93\n")
        uv.chmod(0o755)
        result = subprocess.run(state["recovery_command"],
                                env={**self.env, "UV_OFFLINE": "1", "PATH": str(tools) + os.pathsep + self.env["PATH"]},
                                capture_output=True, text=True, timeout=90)
        self.assertEqual(result.returncode, 0, result.stdout + "\n" + result.stderr)
        self.assertEqual(json.loads(result.stdout)["phase"], "rolled_back")
        replay = subprocess.run(state["recovery_command"], env=self.env, capture_output=True, text=True, timeout=90)
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertTrue(root.exists(), "rollback deleted the retained candidate")
        self.assertTrue(self.target.exists(), "rollback lost the independent predecessor")
        self.assertFalse((self.target.parent / ".production-active.json").exists())
        direct = subprocess.run([str(self.target / "runtime/.venv/bin/devflow-delivery"), "--help"],
                                env=self.env, capture_output=True, text=True)
        self.assertEqual(direct.returncode, 0, direct.stderr)


class FreshNamespaceInstallation(GenerationFixture):
    def test_public_allocator_accepts_current_scope_and_refuses_old_sdk_outbox_access(self):
        request, root = self.prepare_and_apply()
        tracked = root / "runtime/src/devflow_temporal/delivery_store.py"
        original_source = tracked.read_bytes()
        namespaces = self.home / ".local/state/devflow/namespaces"
        before_namespaces = sorted(namespaces.iterdir()) if namespaces.exists() else []
        try:
            tracked.write_bytes(original_source + b"\n# owned harmless source drift\n")
            ordinary = subprocess.run([str(root / "runtime/.venv/bin/devflow-delivery"), "--help"],
                                      env=self.env, capture_output=True, text=True)
            self.assertEqual(ordinary.returncode, 0, ordinary.stderr)
            qualified = self.managed_python(root, "print('must not run')")
            self.assertNotEqual(qualified.returncode, 0)
            self.assertNotIn("must not run", qualified.stdout)
            refused_allocation = self.command("--allocate-namespace", self.config, root=root, check=False)
            self.assertNotEqual(refused_allocation.returncode, 0)
            self.assertEqual(sorted(namespaces.iterdir()) if namespaces.exists() else [], before_namespaces)
        finally:
            tracked.write_bytes(original_source)
        allocated = self.command("--allocate-namespace", self.config, root=root)
        config = Path(allocated["config"])
        witness = json.loads(Path(allocated["witness"]).read_text())
        self.assertEqual(witness["phase"], "verified")
        original = json.loads(self.config.read_text())
        self.assertNotEqual(json.loads(config.read_text())["queue"], original.get("queue", "devflow-delivery"))
        project = self.home / "project.json"
        project.write_text(json.dumps({"version": 1, "owners": [str(config)],
                                       "retained": "x" * (4 * 1024 * 1024)}))
        project.chmod(0o644)
        try:
            tracked.write_bytes(original_source + b"\n# owned namespace-bound console drift\n")
            for name, args in (("devflow-delivery", ["--conf", str(config), "--help"]),
                               ("devflow-delivery", ["--config", "~/" + str(config.relative_to(self.home)), "--help"]),
                               ("devflow-project-sync", ["--config", str(project), "--help"])):
                denied = subprocess.run([str(root / "runtime/.venv/bin" / name), *args],
                                        env=self.env, capture_output=True, text=True)
                self.assertNotEqual(denied.returncode, 0, "namespace console silently used ordinary startup")
                self.assertIn("qualified source", denied.stderr)
        finally:
            tracked.write_bytes(original_source)
        project_help = subprocess.run([str(root / "runtime/.venv/bin/devflow-project-sync"),
                                       "--config", str(project), "--help"],
                                      env=self.env, capture_output=True, text=True)
        self.assertEqual(project_help.returncode, 0, project_help.stderr)
        program = ("import pathlib,sys;from devflow_temporal.delivery_config import DeliveryConfig;"
                   "from devflow_temporal.delivery_installation_owner import fresh_namespace_scope;"
                   "from devflow_temporal.delivery_store import DeliveryStore;"
                   "c=DeliveryConfig.load(pathlib.Path(sys.argv[1]));\n"
                   "with fresh_namespace_scope(c) as scope:\n"
                   " scope.verify();DeliveryStore(c);scope.verify();print(__import__('json').dumps(scope.witness))")
        current = self.managed_python(root, program, str(config))
        self.assertEqual(current.returncode, 0, current.stderr)
        proof = json.loads(current.stdout)
        self.assertEqual(proof["namespace_id"], allocated["namespace_id"])
        self.assertEqual(proof["source_root"], str(root))
        self.assertEqual(proof["source_revision"], self.source_provenance["fixture_head"])
        self.assertEqual(proof["birth_sha256"], witness["birth_sha256"])
        database = Path(witness["database"])
        alternate = self.home / "second-owner.json"
        altered = json.loads(config.read_text())
        altered["max_repairs"] = 3
        alternate.write_text(json.dumps(altered))
        alternate.chmod(0o600)
        alias = database.parent / "state-alias"
        alias.symlink_to(database.parent / "state", target_is_directory=True)
        owner_inventory = self.target.parent / ".production-configurations" / (root.parent.name + ".json")
        before_inventory = owner_inventory.read_bytes()
        reject_caller = ("import json,pathlib,sys;from devflow_temporal.delivery_config import DeliveryConfig;"
                         "from devflow_temporal.delivery_installation_owner import fresh_namespace_scope;"
                         "p=pathlib.Path(sys.argv[1]);raw=json.loads(p.read_text());"
                         "raw.update(state_root=sys.argv[2]) if len(sys.argv)>2 else None;"
                         "c=DeliveryConfig._from_raw(p,raw);\n"
                         "with fresh_namespace_scope(c):print('unexpected admission')")
        for inputs in ([str(alternate)], [str(config), str(alias)]):
            before_namespace = {str(file): file.read_bytes() for file in database.parent.iterdir() if file.is_file()}
            denied = self.managed_python(root, reject_caller, *inputs)
            self.assertNotEqual(denied.returncode, 0)
            self.assertIn("exact allocated config, state and tracking", denied.stderr)
            self.assertEqual(owner_inventory.read_bytes(), before_inventory)
            self.assertEqual({str(file): file.read_bytes() for file in database.parent.iterdir() if file.is_file()},
                             before_namespace)
        mutation = ("import pathlib,sys;from devflow_temporal.delivery_config import DeliveryConfig;"
                    "from devflow_temporal.delivery_installation_owner import fresh_namespace_scope;"
                    "c=DeliveryConfig.load(pathlib.Path(sys.argv[1]));\n"
                    "with fresh_namespace_scope(c) as scope:\n"
                    " c.raw['max_repairs']=37;scope.verify()")
        mutated = self.managed_python(root, mutation, str(config))
        self.assertNotEqual(mutated.returncode, 0)
        self.assertIn("scope changed during its admitted operation", mutated.stderr)
        self.assertEqual(owner_inventory.read_bytes(), before_inventory)
        before = {str(file): file.read_bytes() for file in database.parent.iterdir() if file.is_file()}
        helper = root / "skills/devflow/scripts/state.py"
        older = ("import importlib.util,pathlib,sys;s=importlib.util.spec_from_file_location('state',sys.argv[1]);"
                 "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);m.connect(pathlib.Path(sys.argv[2]))")
        rejected = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c", older,
                                   str(helper), str(database)], env=self.env, capture_output=True, text=True)
        self.assertNotEqual(rejected.returncode, 0)
        self.assertIn("active generation SDK and interpreter", rejected.stderr)
        after = {str(file): file.read_bytes() for file in database.parent.iterdir() if file.is_file()}
        self.assertEqual(after, before)
        # Public registration validates an active generation, while the strict
        # v1 runtime inspector stays unchanged. Everything is staged in HOME;
        # launchctl is never invoked by this fixture.
        import hashlib
        import plistlib

        bins = self.home / "owned-bin"
        bins.mkdir(mode=0o700)
        launchers = {"schema_version": 1, "command_id": "owned-generation-launchers",
                     "bin_dir": str(bins), "runtime_dir": str(root / "runtime"),
                     "source_revision": proof["source_revision"], "source_tree": proof["source_tree"],
                     "config_path": str(config), "config_sha256": hashlib.sha256(config.read_bytes()).hexdigest(),
                     "entry_sha256": {name: hashlib.sha256((root / "runtime/.venv/bin" / name).read_bytes()).hexdigest()
                                      for name in ("devflow-delivery", "devflow-delivery-mcp")},
                     "expected_before": {name: {"type": "absent"} for name in
                                         ("devflow", "devflow-delivery", "devflow-delivery-mcp")}}
        launcher_request = self.home / "launcher-request.json"
        launcher_request.write_text(json.dumps(launchers))
        launcher_request.chmod(0o600)
        request_sha = hashlib.sha256(launcher_request.read_bytes()).hexdigest()
        for operation in ("preflight", "apply"):
            registered = subprocess.run(["sh", str(root / "scripts/install.sh"), "--delivery-launchers", operation,
                                         "--request", str(launcher_request), "--sha256", request_sha],
                                        env=self.env, capture_output=True, text=True, timeout=90)
            self.assertEqual(registered.returncode, 0, registered.stderr)
            self.assertEqual(json.loads(registered.stdout)["state"], "ready" if operation == "preflight" else "applied")
        command = subprocess.run([str(bins / "devflow-delivery"), "--help"], env=self.env,
                                 capture_output=True, text=True)
        self.assertEqual(command.returncode, 0, command.stderr)
        temporal = self.home / "owned-temporal"
        temporal.write_text('#!/bin/sh\nexec /usr/bin/false "$@"\n')
        temporal.chmod(0o755)
        staged = subprocess.run([str(root / "runtime/.venv/bin/python"), "-I", "-S", "-B",
                                 str(root / "scripts/delivery-service.py"),
                                 "--launch-agents", str(self.home / "owned-launch-agents"), "install",
                                 "--config", str(config), "--runtime", str(root / "runtime"),
                                 "--devflow-delivery", str(root / "runtime/.venv/bin/devflow-delivery"),
                                 "--temporal", str(temporal)],
                                env={**self.env, "PATH": "/usr/bin:/bin"}, capture_output=True, text=True, timeout=90)
        self.assertEqual(staged.returncode, 0, staged.stderr)
        service = json.loads(staged.stdout)
        self.assertFalse(service["active"])
        agent = plistlib.loads(Path(service["staged"]).read_bytes())
        self.assertEqual(agent["DevflowRuntime"], str(root / "runtime"))
        self.assertTrue(Path(agent["ProgramArguments"][0]).is_relative_to(self.target.parent / ".production-bootstraps"))
        refused = self.command("--rollback-generation", request, root=root, check=False)
        self.assertNotEqual(refused.returncode, 0)
        self.assertIn("owner_enrollment_changed", refused.stderr)


if __name__ == "__main__":
    unittest.main()
