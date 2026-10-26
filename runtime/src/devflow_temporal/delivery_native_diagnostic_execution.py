"""Purpose-bound native diagnostic resources; original delivery custody stays untouched."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import time
import xml.etree.ElementTree as ET
from contextlib import ExitStack, contextmanager
from copy import deepcopy
from pathlib import Path

from .candidate import candidate_for
from .contracts import canonical_json, digest
from .delivery_metadata_recovery import _immutable
from .delivery_resources import (
    RunResources,
    _identity,
    private_directory,
    read_private,
)


class DiagnosticContext:
    def __init__(self, broker, row):
        from .delivery_native_diagnostic import KIND, _root, _rows

        self.broker, self.store, self.spec = broker, broker.store, broker.spec
        self.row, self.key = row, row["identity"]
        self.snapshot = json.loads(row["snapshot_json"])
        self.root = _root(self.spec, self.key)
        intent_raw = read_private(self.root / "intent.json", raw=True)
        self.intent = json.loads(intent_raw)
        if (
            set(self.intent)
            != {"kind", "command", "request_digest", "snapshot", "spec_sha256", "generation"}
            or self.intent["kind"] != KIND
            or digest(self.intent["command"]) != row["request_digest"]
            or self.intent["request_digest"] != row["request_digest"]
            or self.intent["snapshot"] != self.snapshot
            or self.intent["spec_sha256"] != digest(self.spec)
            or self.snapshot["identity"] != self.key
            or hashlib.sha256(intent_raw).hexdigest() != row["intent_sha256"]
        ):
            raise ValueError("native diagnostic intent lost its public acknowledgement")
        self.state = self.root / self.spec["run_id"]
        self.checkout = self.state / "gates" / str(self.snapshot["iteration"]) / "verify"
        self.resource_spec = {
            "run_id": self.spec["run_id"],
            "state_dir": str(self.state),
            "source_path": str(self.checkout),
            "checkout": str(self.checkout),
            "base_sha": self.spec["base_sha"],
            "branch": self.spec["branch"],
            "provider": self.spec["provider"],
            "policy": deepcopy(self.spec["policy"]),
            "policy_digest": self.spec["policy_digest"],
            "resource_cleanup_version": 1,
            "request_digest": self.spec["request_digest"],
            "native_diagnostic_resource_only": True,
        }
        self._normal_rows = self.snapshot["rows_sha256"]
        with self.store._connect() as db:
            if digest(_rows(db, self.spec["run_id"])) != self._normal_rows:
                raise ValueError("native diagnostic original execution changed before dispatch")

    def resources(self, *, read_only=False):
        value = RunResources(self.resource_spec, read_only=read_only)
        value.diagnostic_context = self
        return value

    def temporal_authority(self, checkout, evidence, check, registry, manifest):
        """Same native subject/scratch checks under the explicitly acknowledged purpose."""
        from .delivery_broker import _git
        from .delivery_resources import same_object
        from .delivery_sandbox import _native_check_key

        self.guard()
        self.subject()
        if (
            registry.spec != self.resource_spec
            or checkout != self.checkout
            or evidence != self.state / "checks" / str(self.snapshot["iteration"])
            or check != self.snapshot["check"]
        ):
            raise ValueError("diagnostic Temporal preparation left its exact subject or check")
        entry = manifest["roots"].get(str(checkout), {})
        candidate = candidate_for(checkout)
        if (
            entry.get("kind") != "checkout"
            or not entry.get("identity")
            or entry.get("receipt", {}).get("state") in {"removed", "already_absent"}
            or not same_object(self.resource_spec, checkout, entry["identity"])
            or _git(checkout, "rev-parse", "--show-toplevel") != str(checkout)
            or any(
                candidate[k] != self.snapshot["candidate"][k]
                for k in ("id", "head", "content_sha256")
            )
            or entry.get("head", candidate["head"]) != candidate["head"]
        ):
            raise ValueError("diagnostic Temporal preparation lost current subject custody")
        registry._allowed(checkout, "checkout")
        key = _native_check_key(self.spec, evidence, check["id"])
        home = self.state / "transient/checks" / key
        recipe = read_private(home / "codex/native-environment.json")
        if type(recipe.get("version")) is not int or recipe["version"] not in (1, 2, 3):
            raise ValueError("diagnostic native environment recipe changed")
        short = recipe["version"] >= 2
        parent = registry._execution_scratch_root() if short else self.state / "transient"
        scratch = parent / digest(["checks", key])[:16] if short else home / "tmp"
        owned = manifest["roots"].get(str(parent), {})
        kind = "execution-scratch" if short else "transient"
        if (
            owned.get("kind") != kind
            or not owned.get("identity")
            or owned.get("receipt", {}).get("state") in {"removed", "already_absent"}
            or not same_object(self.resource_spec, parent, owned["identity"])
            or parent.stat().st_mode & 0o777 != 0o700
            or scratch.resolve(strict=True) != scratch
            or scratch.stat().st_mode & 0o777 != 0o700
        ):
            raise ValueError("diagnostic native scratch lost its owned generation")
        registry._allowed(parent, kind)
        return {
            "key": key,
            "evidence": str(evidence),
            "check_sha256": digest(check),
            "diagnostic_identity": self.key,
            "work_binding": self.snapshot["work_binding"],
            "claim": None,
            "subject": {
                "path": str(checkout),
                "kind": "checkout",
                "identity": entry["identity"],
                "generation": entry.get("generation", 0),
                "candidate": candidate,
                "admitted_candidate": self.snapshot["candidate"],
            },
            "environment_recipe": recipe,
            "scratch": str(scratch),
            "scratch_identity": _identity(scratch),
            "scratch_owner": {
                "path": str(parent),
                "kind": kind,
                "identity": owned["identity"],
                "generation": owned.get("generation", 0),
            },
        }

    def launch_binding(self, process):
        check = self.snapshot["check"]
        if process.folder.parent.name != check["id"]:
            raise ValueError("diagnostic tool launch left its frozen worker check")
        if process.folder.name == "native":
            return check["argv"], check.get("timeout_seconds", 600), {"native"}
        if process.folder.name == "readiness-native":
            return readiness_argv(self), 180, {"readiness-native"}
        raise ValueError("diagnostic invocation is not eligible for a retry generation")

    def temporal_launch(self, process):
        from . import delivery_temporal_prerequisites as temporal
        from .delivery_sandbox import _native_check_key

        self.guard()
        keys = {tool["environment"] for tool in temporal.TOOLS.values()}
        present = keys & process.environment.keys()
        if not present:
            if process.folder.parent.name == self.snapshot["check"]["id"]:
                raise ValueError("diagnostic worker lost its declared Temporal tool environment")
            return  # Existing frozen prerequisite/helper, not the full worker recipe.
        if present != keys or process.spec != self.spec:
            raise ValueError("diagnostic native launch changed its original tool authority")
        registry = self.resources(read_only=True)
        evidence = process.folder.parent.parent
        check = self.snapshot["check"]
        key = _native_check_key(self.spec, evidence, check["id"])
        with registry.locked(read_only=True) as manifest:
            seal = manifest.get("temporal_test_preparations", {}).get(key)
            if (
                not isinstance(seal, dict)
                or seal.get("preparation") != self.snapshot["temporal_marker"]
            ):
                raise ValueError("diagnostic Temporal tools have no resource-owner seal")
            authority = self.temporal_authority(self.checkout, evidence, check, registry, manifest)
            if authority != seal.get("authority"):
                raise ValueError("diagnostic Temporal resource generation changed")
            path = evidence / check["id"] / "temporal-prerequisites.json"
            if seal.get("receipt") != str(path):
                raise ValueError("diagnostic Temporal receipt left its fixed check")
            recorded = json.loads(
                temporal._reference(str(path), seal["receipt_sha256"], private=True)
            )
            temporal._check_owned_launch(
                process, self.snapshot["preparation"], recorded, authority, diagnostic_context=self
            )
            if (
                self.temporal_authority(self.checkout, evidence, check, registry, manifest)
                != authority
            ):
                raise ValueError("diagnostic Temporal custody changed during launch observation")

    def guard(self, *, states=None):
        from .delivery_native_diagnostic import _request, require_generation, snapshot

        if states is None:
            states = getattr(self, "_allowed_states", ("running",))
        require_generation(self.root, self.intent["generation"])
        if (
            hashlib.sha256(read_private(self.root / "intent.json", raw=True)).hexdigest()
            != (self.row["intent_sha256"])
        ):
            raise ValueError("native diagnostic acknowledged intent changed")
        if (
            snapshot(
                self.store, self.spec["run_id"], _request(self.intent["command"], acknowledged=True)
            )
            != self.snapshot
        ):
            raise ValueError("native diagnostic original authority changed")
        with self.store._connect() as db:
            row = db.execute(
                "SELECT * FROM delivery_native_diagnostics WHERE identity=?", (self.key,)
            ).fetchone()
        if (
            row is None
            or row["state"] not in states
            or row["request_digest"] != self.row["request_digest"]
            or row["snapshot_json"] != self.row["snapshot_json"]
            or row["intent_sha256"] != self.row["intent_sha256"]
        ):
            raise ValueError("native diagnostic has no current execution acknowledgement")

    def changed(self):
        """Cheap cancellation predicate; full file/native proof is rechecked at phase boundaries."""
        from .delivery_native_diagnostic import _rows

        try:
            with self.store._connect() as db:
                row = db.execute(
                    "SELECT state FROM delivery_native_diagnostics WHERE identity=?", (self.key,)
                ).fetchone()
                return (
                    row is None
                    or row["state"] != "running"
                    or digest(_rows(db, self.spec["run_id"])) != self._normal_rows
                    or self.store.state.claim_for(db, self.spec["work_id"]) is not None
                )
        except (OSError, ValueError, sqlite3.Error):
            return True

    def create_subject(self):
        self.guard()
        if os.path.lexists(self.state):
            raise ValueError("native diagnostic generation already exists; never replace it")
        self.state.mkdir(mode=0o700)
        resources = self.resources()
        private_directory(self.checkout.parent)
        # This is an independent primary clone. The existing checkout owner
        # retains blocked source; a disposable Git worktree/gate owner would
        # correctly refuse trying to remove the primary clone as a worktree.
        resources.register(self.checkout, "checkout")
        environment = {
            **os.environ,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_TERMINAL_PROMPT": "0",
        }
        clone = [
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "clone",
            "--no-hardlinks",
            "--dissociate",
            "--no-checkout",
            self.spec["source_path"],
            str(self.checkout),
        ]
        subprocess.run(clone, env=environment, check=True, capture_output=True, timeout=120)
        for arguments in [
            ["checkout", "--detach", self.snapshot["candidate"]["head"]],
            ["remote", "set-url", "origin", self.spec["origin_url"]],
        ]:
            subprocess.run(
                [
                    "git",
                    "-c",
                    "core.hooksPath=/dev/null",
                    "-c",
                    "core.fsmonitor=false",
                    "-C",
                    str(self.checkout),
                    *arguments,
                ],
                env=environment,
                check=True,
                capture_output=True,
                timeout=120,
            )
        resources.created(self.checkout)
        if any(
            candidate_for(self.checkout)[key] != self.snapshot["candidate"][key]
            for key in ("id", "head", "content_sha256")
        ):
            raise ValueError("native diagnostic copy differs from the original published candidate")
        self.guard()
        _immutable(
            self.root / "subject.json",
            {
                "path": str(self.checkout),
                "identity": _identity(self.checkout),
                "candidate": self.snapshot["candidate"],
                "resource_key": self.key,
            },
        )
        _record_phase(
            self,
            "subject_sha256",
            hashlib.sha256(read_private(self.root / "subject.json", raw=True)).hexdigest(),
        )

    def subject(self):
        from .delivery_resources import same_object

        raw = read_private(self.root / "subject.json", raw=True)
        saved = json.loads(raw)
        with self.store._connect() as db:
            phase = json.loads(
                db.execute(
                    "SELECT response_json FROM delivery_native_diagnostics WHERE identity=?",
                    (self.key,),
                ).fetchone()[0]
            )
        if (
            hashlib.sha256(raw).hexdigest() != phase.get("subject_sha256")
            or saved.get("path") != str(self.checkout)
            or saved.get("resource_key") != self.key
            or saved.get("candidate") != self.snapshot["candidate"]
            or not same_object(self.resource_spec, self.checkout, saved["identity"])
            or any(
                candidate_for(self.checkout)[key] != saved["candidate"][key]
                for key in ("id", "head", "content_sha256")
            )
        ):
            raise ValueError("native diagnostic current subject lost its exact owned generation")
        observed = subprocess.run(
            ["/usr/sbin/lsof", "-nP", "-F", "p", "+D", str(self.checkout)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if observed.returncode not in (0, 1) or any(
            line.startswith("p") and line[1:].isdecimal() for line in observed.stdout.splitlines()
        ):
            raise ValueError("native diagnostic subject has a live user or unreadable custody")
        return saved

    def attach(self):
        self.broker.diagnostic_context = self
        self.broker.checkout = self.checkout
        self.broker.source = self.checkout
        self.broker.state_dir = self.state
        self.broker.evidence_dir = self.state
        cancelled = self.broker.check_cancelled
        self.broker.check_cancelled = lambda: cancelled() or self.changed()

    def process(self, process):
        if (
            process.spec != self.spec
            or process.register_resources
            or not process.folder.is_relative_to(self.state)
            or process.folder.resolve() != process.folder
            or process.environment.get("PLAYWRIGHT_BROWSERS_PATH")
            != self.snapshot["controller_browser_binding"]
        ):
            raise ValueError("native diagnostic process left its exact scoped generation")
        process.diagnostic_context = self
        self.guard()
        self.subject()
        folder = self.root / "native-launches"
        private_directory(folder)
        binding = {
            "folder": str(process.folder),
            "intent": process._intent(),
            "spec_sha256": digest(self.spec),
            "identity": self.key,
        }
        name = digest(str(process.folder)) + ".json"
        _immutable(folder / name, binding)
        _record_phase(
            self,
            "native_launches",
            {name: hashlib.sha256(read_private(folder / name, raw=True)).hexdigest()},
        )

    def observed_process(self, process):
        if os.path.lexists(process.journal):
            self.resources().process(process.journal)

    def finalize(self):
        self.guard()
        self.subject()
        journals = self.completed_processes()
        for journal in journals:
            self.resources().process(Path(journal))
        receipt = self.resources().finalize("blocked")
        if (
            receipt.get("state") != receipt.get("resource_cleanup")
            or receipt.get("state") != "confirmed"
            or receipt.get("process_cleanup") != "observed-native-confirmed"
        ):
            raise ValueError("native diagnostic resource finalization remains unknown")
        self.guard()
        from .delivery_resources import observe_finalized_resources

        observation = observe_finalized_resources(self.resource_spec)
        return {"resources": receipt, "journal_sha256": journals, "observation": observation}

    def completed_processes(self):
        """Pure observation before the finalizer: never signal an uncertain native actor."""
        import fcntl

        from .delivery_native_process import listeners, process_table

        folder = self.root / "native-launches"
        journals = {}
        table = process_table()
        with self.store._connect() as db:
            phase = json.loads(
                db.execute(
                    "SELECT response_json FROM delivery_native_diagnostics WHERE identity=?",
                    (self.key,),
                ).fetchone()[0]
            )
        expected = phase.get("native_launches", {})
        actual = set(p.name for p in folder.iterdir()) if folder.exists() else set()
        if actual != set(expected):
            raise ValueError("diagnostic native plan inventory is incomplete or changed")
        if folder.exists():
            for path in sorted(folder.iterdir()):
                plan_raw = read_private(path, raw=True)
                if hashlib.sha256(plan_raw).hexdigest() != expected.get(path.name):
                    raise ValueError("diagnostic acknowledged native plan changed")
                launch = json.loads(plan_raw)
                journal = Path(launch["folder"]) / "native-process.json"
                if (
                    launch["identity"] != self.key
                    or launch["spec_sha256"] != digest(self.spec)
                    or not journal.is_relative_to(self.state)
                    or not os.path.lexists(journal)
                ):
                    raise ValueError("native diagnostic launch remains unknown")
                raw = read_private(journal, raw=True)
                value = json.loads(raw)
                if (
                    value.get("intent") != launch["intent"]
                    or value.get("phase") != "finished"
                    or value.get("monitoring_complete") is not True
                    or value.get("result", {}).get("cleanup") != "observed-native-confirmed"
                    or not value.get("owned")
                    or any(
                        table.get(int(pid), {}).get("identity") == actor["identity"]
                        and not table[int(pid)]["stat"].startswith("Z")
                        for pid, actor in value["owned"].items()
                    )
                    or any(listeners(port) for port in value.get("ports", []))
                ):
                    raise ValueError("native diagnostic monitor did not confirm its own completion")
                for name in ("native-process.lock", "native-monitor.lock"):
                    fd = os.open(
                        journal.with_name(name), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
                    )
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    finally:
                        os.close(fd)
                journals[str(journal)] = hashlib.sha256(raw).hexdigest()
        return journals


@contextmanager
def execution_lease(spec, row):
    """A second controller may observe this invocation, never replace its body."""
    from .delivery_native_diagnostic import _root, generation_lease

    root = _root(spec, row["identity"])
    raw = read_private(root / "intent.json", raw=True)
    if hashlib.sha256(raw).hexdigest() != row["intent_sha256"]:
        raise ValueError("native diagnostic execution intent changed")
    with generation_lease(root, expected=json.loads(raw)["generation"]):
        yield


def execute(broker, row):
    """Only a queued acknowledged identity can enter a fresh native generation."""
    from .delivery_native_diagnostic import info

    deadline = time.monotonic() + 5
    with ExitStack() as leases:
        while True:
            try:
                leases.enter_context(execution_lease(broker.spec, row))
                break
            except BlockingIOError:
                # The ACK transaction becomes visible just before its held
                # generation FD is released. Wait for that short publication
                # window; another running body is observed, never superseded.
                with broker.store._connect() as db:
                    state = db.execute(
                        "SELECT state FROM delivery_native_diagnostics WHERE identity=?",
                        (row["identity"],),
                    ).fetchone()
                if state is None or state[0] != "queued":
                    return info(broker.store, row["run_id"], row["command_id"])
                if time.monotonic() >= deadline:
                    raise ValueError("diagnostic queued generation lease remains owned") from None
                time.sleep(0.03)
        return _execute(broker, row)


def _execute(broker, row):
    from .delivery_native_diagnostic import record_unknown
    from .delivery_preparation import verify_prepared_spec

    store = broker.store
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        updated = db.execute(
            "UPDATE delivery_native_diagnostics SET state='running' "
            "WHERE identity=? AND state='queued'",
            (row["identity"],),
        ).rowcount
    if updated != 1:
        raise ValueError("native diagnostic is already running or uncertain; never relaunch it")
    context = None
    try:
        context = DiagnosticContext(broker, row)
        verify_prepared_spec(broker.spec)
        context.create_subject()
        context.attach()
        candidate = {**broker.candidate(), "policy_digest": broker.spec["policy_digest"]}
        preparation = broker.run_implementation_preparation(
            context.snapshot["iteration"], candidate
        )
        if preparation["state"] != "passed" or preparation.get("cleanup") != "confirmed":
            result = {"state": "failed", "stage": "preparation", "preparation": preparation}
        else:
            readiness = run_readiness(context)
            if readiness["state"] != "passed":
                result = {"state": "failed", "stage": "readiness", "readiness": readiness}
            else:
                context.guard()
                result = {
                    "state": "finished",
                    "stage": "frozen_check",
                    "readiness": readiness,
                    "check": broker._run_check_list(
                        broker.checkout,
                        [context.snapshot["check"]],
                        context.state / "checks" / str(context.snapshot["iteration"]),
                        candidate,
                    ),
                }
        if result["stage"] == "frozen_check":
            result["state"] = (
                "passed"
                if result["check"].get("state") == "passed"
                else result["check"].get("state", "failed")
            )
        _save_result(context, result)
        cleanup = context.finalize()
        receipt = {
            "kind": "native_check_diagnostic",
            "identity": context.key,
            "run_id": broker.spec["run_id"],
            "command_id": row["command_id"],
            "original_snapshot_sha256": digest(context.snapshot),
            "result": result,
            "cleanup": cleanup,
            "artifacts": retained_artifacts(context, result),
            "diagnostic_only": True,
            "delivery_acceptance": False,
            "provider_turns": 0,
        }
        _immutable(context.root / "receipt.json", receipt)
        context.guard()
        return _ack_complete(store, row, receipt)
    except Exception as exc:
        # Nothing uncertain grants a replacement invocation or cleanup. Keep the
        # same durable identity/fence; an immutable finished receipt is replayable.
        record_unknown(store, row, exc)
        from .delivery_native_diagnostic import info

        return info(store, row["run_id"], row["command_id"])


SELECTORS = (
    "tests/test_linkedin_guest_dialog.py::"
    "test_offered_guest_dialog_is_closed_before_description_capture",
    "tests/test_saved_posting_availability_runtime.py::"
    "test_real_temporal_activity_and_registered_workflow_record_current_evidence",
    "tests/test_workflow_apply.py::test_apply_workflow_returns_ok_when_apply_main_succeeds",
    "tests/test_workflow_durability_probe.py::test_durability_probe_resumes_same_run_after_worker_crash",
)


def readiness_argv(context):
    """Fixed controller readiness, using the unchanged worker wrapper's child environment."""
    report = context.state / "checks" / str(context.snapshot["iteration"]) / "worker-regressions"
    report = report / "readiness/junit.xml"
    program = (
        "import path from 'node:path';import fs from 'node:fs';"
        "import {spawnSync} from 'node:child_process';"
        "const value=process.env.PYTEST_ADDOPTS||'';"
        "if(!value.startsWith('--basetemp='))throw Error('controller artifact path missing');"
        "const root=value.slice(11).replace(/^'|'$/g,'');"
        "fs.mkdirSync(root,{recursive:true});"
        "const report=" + json.dumps(str(report)) + ";"
        "fs.mkdirSync(path.dirname(report),{recursive:true});"
        "const env={...process.env,PYTHONPATH:path.join(process.cwd(),'workers/automation/src')};"
        "const argv=['-m','pytest','-q','-n','2','--dist=loadfile','--max-worker-restart=0',"
        "'-m','not system_browser','--basetemp='+path.join(root,'readiness'),"
        "'--junitxml='+report,..." + json.dumps(list(SELECTORS)) + "];"
        "const child=spawnSync('./.venv/bin/python',argv,"
        "{stdio:'inherit',cwd:path.resolve('workers/automation'),env});"
        "if(child.error)throw child.error;process.exit(child.status??1);"
    )
    return ["node", "--input-type=module", "-e", program]


def run_readiness(context):
    from .delivery_check_evidence import _owned_bytes
    from .delivery_sandbox import native_check_argv, prepare_native_check

    broker, check = context.broker, context.snapshot["check"]
    evidence = context.state / "checks" / str(context.snapshot["iteration"])
    context.guard()
    profile, environment = prepare_native_check(
        context.spec,
        context.checkout,
        evidence,
        check,
        resources=context.resources(),
        diagnostic_context=context,
    )
    process = broker._native_process(
        evidence / check["id"] / "readiness-native",
        argv=native_check_argv(context.spec, profile, context.checkout, readiness_argv(context)),
        cwd=context.checkout,
        environment=environment,
        timeout=180,
        cancelled=broker._native_cancelled,
    )
    outcome = broker._run_native_check(process)
    if (
        outcome.get("cleanup") != "observed-native-confirmed"
        or outcome.get("interrupted_monitor") is True
    ):
        raise ValueError("diagnostic readiness monitoring is unknown; it cannot be repeated")
    path = evidence / check["id"] / "readiness/junit.xml"
    report, error, passed = None, None, False
    try:
        raw = _owned_bytes(path)
        if len(raw) > 2 * 1024 * 1024:
            raise ValueError("diagnostic readiness report exceeds its finite bound")
        parsed = ET.fromstring(raw)
        cases = list(parsed.iter("testcase"))
        expected = {(s.split("::")[0][:-3].replace("/", "."), s.split("::")[1]) for s in SELECTORS}
        actual = [(c.get("classname"), c.get("name")) for c in cases]
        passed = (
            len(actual) == len(expected)
            and set(actual) == expected
            and not any(
                c.findall("error") or c.findall("failure") or c.findall("skipped") for c in cases
            )
            and outcome.get("exit_code") == 0
            and not outcome.get("timed_out")
            and not outcome.get("cancelled")
        )
        report = {
            "path": str(path),
            "sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw),
            "selectors": list(SELECTORS),
            "test_cases": [list(pair) for pair in actual],
        }
    except (OSError, ValueError, ET.ParseError) as exc:
        error = str(exc)[:500]
    result = {
        "state": "passed" if passed else "failed",
        "native_process": outcome,
        "environment_sha256": digest(environment),
        "junit": report,
        "error": error,
        "browser_binding": environment.get("PLAYWRIGHT_BROWSERS_PATH"),
        "original_check_sha256": digest(check),
        "acceptance_gate": False,
    }
    context.guard()
    context.subject()
    _immutable(context.root / "readiness.json", result)
    return result


def retained_artifacts(context, result):
    """Retain bounded exact evidence references; no provider-controlled path walk."""
    from .delivery_check_evidence import _owned_bytes

    paths = {
        context.state / "resources/manifest.json",
        context.state / "resources/finalization.json",
    }
    for binding in sorted((context.root / "native-launches").glob("*.json")):
        launch = read_private(binding)
        folder = Path(launch["folder"])
        if not folder.is_relative_to(context.state):
            raise ValueError("diagnostic evidence left its exact namespace")
        paths.update([folder / "native-process.json", folder / "process.log"])
    readiness = result.get("readiness", {}).get("junit")
    if readiness:
        paths.add(Path(readiness["path"]))
    # The full check owner already authenticates each retained artifact against
    # its candidate. Preserve all its bytes, not just a passing summary.
    for manifest in context.state.glob("checks/*/worker-regressions/artifacts.json"):
        paths.add(manifest)
        value = read_private(manifest)
        paths.update(manifest.parent / "artifacts" / p["relative_path"] for p in value["artifacts"])
    if len(paths) > 4200:
        raise ValueError("diagnostic evidence exceeds its finite file count")
    references, total = {}, 0
    for path in sorted(paths):
        if not path.is_relative_to(context.state):
            raise ValueError("diagnostic evidence path is outside its owned generation")
        raw = _owned_bytes(path)
        total += len(raw)
        if total > 96 * 1024 * 1024:
            raise ValueError("diagnostic evidence exceeds its finite aggregate bound")
        references[path.relative_to(context.state).as_posix()] = {
            "sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw),
        }
    return references


def _record_phase(context, field, value):
    """Files remain inert until their exact subject/launch/result bytes are acknowledged."""
    if field not in {"subject_sha256", "native_launches", "phase_result_sha256"}:
        raise ValueError("unsupported diagnostic phase acknowledgement")
    from .delivery_native_diagnostic import _rows

    with context.store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT * FROM delivery_native_diagnostics WHERE identity=?", (context.key,)
        ).fetchone()
        if (
            row is None
            or row["state"] != "running"
            or row["request_digest"] != context.row["request_digest"]
            or row["snapshot_json"] != context.row["snapshot_json"]
            or row["intent_sha256"] != context.row["intent_sha256"]
            or digest(_rows(db, context.spec["run_id"])) != context.snapshot["rows_sha256"]
            or context.store.state.claim_for(db, context.spec["work_id"]) is not None
        ):
            raise ValueError("diagnostic phase lost its unchanged acknowledged authority")
        response = json.loads(row["response_json"])
        if field == "native_launches":
            before = response.get(field, {})
            if len(before.keys() | value.keys()) > 64 or any(
                key in before and before[key] != sha for key, sha in value.items()
            ):
                raise ValueError("diagnostic native generation changed or exceeded its bound")
            value = {**before, **value}
        elif field in response and response[field] != value:
            raise ValueError("diagnostic phase conflicts with its immutable acknowledgement")
        response.update(state="running", **{field: value})
        db.execute(
            "UPDATE delivery_native_diagnostics SET response_json=? WHERE identity=?",
            (canonical_json(response), context.key),
        )


def _save_result(context, result):
    path = context.root / "execution-result.json"
    _immutable(
        path,
        {"identity": context.key, "snapshot_sha256": digest(context.snapshot), "result": result},
    )
    sha = hashlib.sha256(read_private(path, raw=True)).hexdigest()
    _record_phase(context, "phase_result_sha256", sha)


def verify_receipt(spec, row, receipt):
    from .delivery_check_evidence import _owned_bytes
    from .delivery_native_diagnostic import _root

    root = _root(spec, row["identity"])
    intent_raw = read_private(root / "intent.json", raw=True)
    intent = json.loads(intent_raw)
    if (
        receipt.get("identity") != row["identity"]
        or receipt.get("run_id") != row["run_id"]
        or receipt.get("command_id") != row["command_id"]
        or receipt.get("original_snapshot_sha256") != digest(json.loads(row["snapshot_json"]))
        or intent.get("snapshot") != json.loads(row["snapshot_json"])
        or intent.get("request_digest") != row["request_digest"]
        or digest(intent.get("command")) != row["request_digest"]
        or hashlib.sha256(intent_raw).hexdigest() != row["intent_sha256"]
        or receipt.get("diagnostic_only") is not True
        or receipt.get("delivery_acceptance") is not False
        or receipt.get("provider_turns") != 0
    ):
        raise ValueError("diagnostic immutable receipt lost its acknowledgement")
    phase_sha = json.loads(row["response_json"]).get("phase_result_sha256")
    phases = json.loads(row["response_json"])
    subject_sha = phases.get("subject_sha256")
    if (
        not subject_sha
        or hashlib.sha256(read_private(root / "subject.json", raw=True)).hexdigest() != subject_sha
    ):
        raise ValueError("diagnostic subject lost its original acknowledged generation")
    plans = phases.get("native_launches", {})
    folder = root / "native-launches"
    actual = set(p.name for p in folder.iterdir()) if folder.exists() else set()
    if actual != set(plans) or len(plans) > 64:
        raise ValueError("diagnostic native plan inventory changed")
    for name, expected in plans.items():
        if name != Path(name).name or hashlib.sha256(
            read_private(folder / name, raw=True)
        ).hexdigest() != (expected):
            raise ValueError("diagnostic acknowledged native plan changed")
    if phase_sha:
        phase = read_private(root / "execution-result.json", raw=True)
        value = json.loads(phase)
        if hashlib.sha256(phase).hexdigest() != phase_sha or value != {
            "identity": row["identity"],
            "snapshot_sha256": receipt["original_snapshot_sha256"],
            "result": receipt["result"],
        }:
            raise ValueError("diagnostic receipt differs from its acknowledged phase result")
    elif receipt["result"].get("state") != "unknown":
        raise ValueError("unacknowledged diagnostic execution cannot claim a verdict")
    state = root / spec["run_id"]
    artifacts = receipt.get("artifacts")
    if not isinstance(artifacts, dict) or not 2 <= len(artifacts) <= 4200:
        raise ValueError("diagnostic receipt has no complete bounded evidence")
    total = 0
    for relative, expected in artifacts.items():
        path = Path(relative)
        if path.is_absolute() or path.as_posix() != relative or ".." in path.parts:
            raise ValueError("diagnostic receipt has an invalid evidence path")
        raw = _owned_bytes(state / path)
        total += len(raw)
        if (
            total > 96 * 1024 * 1024
            or len(raw) != expected.get("bytes")
            or hashlib.sha256(raw).hexdigest() != expected.get("sha256")
        ):
            raise ValueError("diagnostic retained evidence changed")


def _ack_complete(store, row, receipt):
    from .delivery_native_diagnostic import _root, _rows

    spec = store.effective_spec(row["run_id"])
    with store._connect() as db:
        current = dict(
            db.execute(
                "SELECT * FROM delivery_native_diagnostics WHERE identity=?", (row["identity"],)
            ).fetchone()
        )
    verify_receipt(spec, current, receipt)
    response = {
        "kind": "native_check_diagnostic",
        "state": "complete",
        "run_id": row["run_id"],
        "command_id": row["command_id"],
        "identity": row["identity"],
        "diagnostic_only": True,
        "delivery_acceptance": False,
        "provider_turns": 0,
        "receipt_sha256": hashlib.sha256(
            read_private(_root(spec, row["identity"]) / "receipt.json", raw=True)
        ).hexdigest(),
    }
    phases = json.loads(current["response_json"])
    for name in ("subject_sha256", "native_launches", "phase_result_sha256"):
        if name in phases:
            response[name] = phases[name]
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        if (
            digest(_rows(db, row["run_id"])) != json.loads(row["snapshot_json"])["rows_sha256"]
            or store.state.claim_for(db, row["work_id"]) is not None
        ):
            raise ValueError("diagnostic original execution changed before receipt acknowledgement")
        changed = db.execute(
            "UPDATE delivery_native_diagnostics SET state='complete',response_json=? "
            "WHERE identity=? AND request_digest=? AND state IN ('running','unknown')",
            (canonical_json(response), row["identity"], row["request_digest"]),
        ).rowcount
        if changed != 1:
            raise ValueError("diagnostic receipt has no unfinished exact acknowledgement")
        db.execute(
            "UPDATE delivery_commands SET response_json=? WHERE command_id=?",
            (canonical_json(response), row["command_id"]),
        )
    return response


def reconcile_complete(store, row):
    """Only acknowledgement loss after a complete immutable receipt can settle."""
    from .delivery_broker import DeliveryBroker
    from .delivery_native_diagnostic import _root
    from .delivery_resources import observe_finalized_resources

    if row["state"] in ("queued", "complete"):
        return
    spec = store.effective_spec(row["run_id"])
    receipt = _root(spec, row["identity"]) / "receipt.json"
    with execution_lease(spec, row):
        context = DiagnosticContext(DeliveryBroker(store, spec), row)
        context._allowed_states = ("running", "unknown")
        context.guard(states=("running", "unknown"))
        if not os.path.lexists(receipt):
            # An existing launch without a complete monitor remains unknown;
            # this never calls NativeProcess.run, resends GO or kills an actor.
            if not os.path.lexists(context.root / "subject.json"):
                return
            context.subject()
            context.completed_processes()
            phase = context.root / "execution-result.json"
            saved = json.loads(row["response_json"]).get("phase_result_sha256")
            result = {
                "state": "unknown",
                "reason": "controller result was not acknowledged",
                "acceptance_gate": False,
            }
            if saved:
                raw = read_private(phase, raw=True)
                observed = json.loads(raw)
                if (
                    hashlib.sha256(raw).hexdigest() != saved
                    or observed.get("identity") != context.key
                    or observed.get("snapshot_sha256") != digest(context.snapshot)
                ):
                    raise ValueError("diagnostic acknowledged phase result changed")
                result = observed["result"]
            cleanup = context.finalize()
            _immutable(
                receipt,
                {
                    "kind": "native_check_diagnostic",
                    "identity": context.key,
                    "run_id": row["run_id"],
                    "command_id": row["command_id"],
                    "original_snapshot_sha256": digest(context.snapshot),
                    "result": result,
                    "cleanup": cleanup,
                    "artifacts": retained_artifacts(context, result),
                    "diagnostic_only": True,
                    "delivery_acceptance": False,
                    "provider_turns": 0,
                },
            )
        value = read_private(receipt)
        verify_receipt(spec, row, value)
        context.subject()
        if observe_finalized_resources(context.resource_spec) != value["cleanup"]["observation"]:
            raise ValueError("diagnostic cleanup generation changed after receipt publication")
        context.guard(states=("running", "unknown"))
        _ack_complete(store, row, value)
