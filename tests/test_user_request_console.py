"""Unpatched console admission, real check execution and fresh-process continuation."""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest
from domain.helpers import contract, record
from test_user_request_admission import ready_request, user_request

from devflow import __version__
from devflow.adapters.git import GitRepository
from devflow.adapters.sqlite_store import SQLiteStore
from devflow.installation import apply_install, plan_install
from devflow.profiles import digest as profile_digest
from devflow.profiles import load_profile
from devflow.validation import canonical_json, digest


def git(path, *args):
    result = subprocess.run(
        ["git", "-C", str(path), *args], capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def initialize_git(path):
    git(path, "init", "-q", "-b", "main")
    git(path, "config", "user.name", "Synthetic Console Fixture")
    git(path, "config", "user.email", "synthetic@example.invalid")


@pytest.fixture(scope="module")
def installed(tmp_path_factory):
    """Install actual current code into temporary, verified immutable release files."""
    temporary = tmp_path_factory.mktemp("request-console-release")
    source = temporary / "source"
    source.mkdir()
    root = Path(__file__).resolve().parents[1]
    for name in ["src", "skills"]:
        shutil.copytree(root / name, source / name, ignore=shutil.ignore_patterns("__pycache__"))
    for name in ["pyproject.toml", "uv.lock"]:
        shutil.copyfile(root / name, source / name)
    initialize_git(source)
    git(source, "add", ".")
    git(source, "commit", "-qm", "test: installed direct-request console fixture")
    target = temporary / "host-skill"
    manifest = plan_install(
        source, git(source, "rev-parse", "HEAD"), temporary / "managed",
        links={target: "skills/devflow"}, owned_paths=[target],
    )
    apply_install(
        manifest, approved_paths=[target], approved_root=temporary / "managed",
        approved_plan_id=manifest["plan_id"],
    )
    return Path(manifest["release_dir"]), manifest["revision"]


class Console:
    def __init__(self, temporary, installed):
        self.release, self.package_revision = installed
        self.root = temporary / "repository"
        shutil.copytree(Path(__file__).resolve().parents[1] / "fixtures/repositories/prose", self.root)
        initialize_git(self.root)
        self.identity = GitRepository(self.root).identity()
        self.counter = temporary / "check-invocations.txt"
        script = self.root / "scripts/check_readme.py"
        script.write_text(script.read_text() + f"\np = Path({str(self.counter)!r})\np.write_text(p.read_text() + 'run\\n' if p.exists() else 'run\\n')\n")
        profile = self.root / ".devflow"
        (profile / "repository.toml").write_text(
            'schema_version=1\n[repository]\nid=' + json.dumps(self.identity)
            + '\ndefault_branch="main"\n'
        )
        (profile / "workflow.lock").write_text(
            f'schema_version=1\nversion="{__version__}"\nrevision="{self.package_revision}"\n'
        )
        # Select the current test interpreter, independent of host python3 aliases.
        checks = (profile / "checks.toml").read_text().replace('"python3"', json.dumps(sys.executable))
        (profile / "checks.toml").write_text(checks)
        git(self.root, "add", ".")
        git(self.root, "commit", "-qm", "test: synthetic accepted content invariant")
        self.base = git(self.root, "rev-parse", "HEAD")
        git(self.root, "switch", "-qc", "test/request")
        self.state_dir = temporary / "private-state"
        self.revision = 0
        self.sequence = 0
        self.contract = contract()
        self.contract.update(
            endpoint={"kind": "local", "target": str(self.root)},
            scope={"paths": ["README.md"], "boundaries": ["Temporary fixture only"]},
            verification={"recipes": ["prose"], "scenarios": ["A01"], "documentation_owners": []},
        )

    def call(self, command, request=None, *, error=None, env=None):
        result = subprocess.run(
            [sys.executable, "-m", "devflow.cli", *command.split("."), "--request-file", "-",
             "--repository", str(self.root), "--state-dir", str(self.state_dir), "--json"],
            input=json.dumps(request or {}), capture_output=True, text=True, check=False, timeout=30,
            cwd=self.root,
            env={**os.environ, "PYTHONPATH": str(self.release / "src"),
                 "PYTHONDONTWRITEBYTECODE": "1", **(env or {})},
        )
        response = json.loads(result.stdout)
        if error:
            assert result.returncode == 2, response
            assert response["error"]["code"] == error, response
            return response["error"]
        expected_status = 2 if response.get("result", {}).get("status") == "BLOCKED" else 0
        assert result.returncode == expected_status and response["ok"], response
        value = response["result"]
        self.revision = value.get("revision", self.revision)
        return value

    def mutate(self, command, **fields):
        self.sequence += 1
        request = {
            "operation_id": f"console-op-{self.sequence}", "work_id": self.contract["work_id"],
            "expected_revision": self.revision, **fields,
        }
        return self.call(command, request)

    def show(self):
        return self.call("work.show", {"work_id": self.contract["work_id"]})

    def start(self, operations=None):
        self.ready_request = ready_request(
            self.contract, user_request=user_request(allowed_operations=operations or ["edit", "check"]),
        )
        self.ready = self.call("work.ready", self.ready_request)
        snapshot = self.call("snapshot.capture", {
            "snapshot_id": "console-snapshot", "effective_settings": {
                "model": "synthetic-model", "reasoning_effort": "synthetic-effort",
                "service_tier": "synthetic-tier", "source_reference": "synthetic:observed-settings",
            },
        })
        attempt = record(
            "attempt", attempt_id="console-attempt", work_id=self.contract["work_id"],
            scope_hash=self.ready["scope_hash"], authority_id=self.ready["authority_id"],
            host_id="synthetic-host", owner_task_id="synthetic-owner", phase="implement",
            blocker=None, workflow_snapshot_id=snapshot["snapshot_id"],
            model_policy_snapshot_id=snapshot["snapshot_id"], revision=self.revision,
            started_at=datetime.now(UTC).isoformat(), status="active",
        )
        self.start_request = {
            "operation_id": "console-start", "work_id": self.contract["work_id"],
            "expected_revision": self.revision, "record": attempt, "workflow_snapshot": snapshot,
        }
        self.started = self.call("work.start", self.start_request)
        return self.started["action"]

    def capture_candidate(self):
        action = self.start()
        observation = self.call("workspace.register", {
            "work_id": self.contract["work_id"], "action_id": action["action_id"],
            "ownership_token": "synthetic-console-owner", "expected_head": self.base,
            "base_ref": self.base,
        })["observation"]
        self.mutate("action.begin", action_id=action["action_id"])
        receipt = record(
            "action_receipt", action_id=action["action_id"], attempt_id="console-attempt",
            operation=action["operation"], payload_hash=action["payload_hash"],
            expected_revision=action["expected_revision"], status="confirmed",
            external_id=observation["path"], observations=["Actual temporary checkout registration"],
            recorded_at=datetime.now(UTC).isoformat(),
        )
        self.mutate("action.record", record=receipt, observation=observation)
        (self.root / "README.md").write_text("Reviewed local handoff.\n")
        git(self.root, "add", "README.md")
        git(self.root, "commit", "-qm", "docs: apply requested local handoff")
        return self.mutate(
            "candidate.capture", candidate_id="console-candidate", base_ref=self.base,
            dependency_hash=digest("fixture dependencies"), environment_hash=digest(sys.version),
            ownership_token="synthetic-console-owner",
        )


@pytest.fixture
def console(tmp_path, installed):
    return Console(tmp_path, installed)


def test_unpatched_console_ready_start_real_check_and_fresh_process_replay(console):
    doctor = console.call("doctor")
    assert doctor["status"] == "READY"
    assert doctor["execution_admission"] == "direct_user_request"
    assert doctor["authorization"] == "requires_recorded_user_request"
    console.capture_candidate()
    request = {
        "operation_id": "console-check", "work_id": console.contract["work_id"],
        "expected_revision": console.revision, "recipe_id": "prose", "acceptance_ids": ["A01"],
    }
    checked = console.call("check.run", request)
    assert checked["evidence"]["execution_status"] == "PASS"
    assert console.counter.read_text() == "run\n"
    artifact = json.loads((console.state_dir / "artifacts" / checked["evidence"]["artifact_hash"]).read_text())
    assert "PROSE_INVARIANT_OBSERVED" in artifact["output"]
    revision = console.revision
    # Every invocation starts another interpreter, including replay and continuation.
    assert console.call("work.ready", console.ready_request) == console.ready
    assert console.call("work.start", console.start_request) == console.started
    assert console.call("check.run", request) == checked
    state = console.show()
    assert state["revision"] == revision
    assert console.counter.read_text() == "run\n"
    assert state["attempt"]["attempt_id"] == "console-attempt"
    assert state["admission_id"] == console.ready["admission_id"]
    console.mutate("work.reconcile")
    assert console.show()["revision"] == revision + 1


@pytest.mark.parametrize("stop,error", [("cancel", "invalid_state"), ("block", "blocked_work")])
def test_stopped_attempt_cannot_dispatch(console, stop, error):
    action = console.start()
    fields = {"authority_reference": "conversation:stop"} if stop == "cancel" else {
        "blocker": {"code": "fixture", "reason": "Requested pause", "next_action": "Resolve pause"},
    }
    console.mutate("work." + stop, **fields)
    console.call("action.dispatch", {
        "work_id": console.contract["work_id"], "operation_id": "cannot-dispatch",
        "expected_revision": console.revision, "action_id": action["action_id"],
    }, error=error)
    state = console.show()
    assert state["actions"][action["action_id"]]["status"] == "prepared"
    assert not console.counter.exists()


@pytest.mark.parametrize("fields,error", [
    ({"user_request": None}, "invalid_user_request"),
    ({"user_request": user_request(summary=" ")}, "invalid_user_request"),
    ({"user_request": user_request(unknown=True)}, "invalid_user_request"),
    ({"user_request": user_request(allowed_operations=["execute_everything"])}, "invalid_user_request"),
    ({"user_request": user_request(), "authority": "invented"}, "invalid_request"),
    ({"labels": ["ready", "P1"], "queue": "P1", "approved": True}, "user_request_required"),
    ({"authority": "invented", "admission_id": "old-pin"}, "user_request_required"),
    ({}, "user_request_required"),
])
def test_console_rejects_missing_malformed_and_legacy_requests(console, fields, error):
    payload = {key: value for key, value in ready_request(console.contract).items() if key != "user_request"}
    console.call("work.ready", {**payload, **fields}, error=error)
    assert console.call("work.list")["works"] == []


def test_console_cannot_expand_operation_or_repository_binding(console, tmp_path):
    console.start(operations=["edit"])
    console.call("check.run", {
        "operation_id": "unadmitted-check", "work_id": console.contract["work_id"],
        "expected_revision": console.revision, "recipe_id": "prose", "acceptance_ids": ["A01"],
    }, error="admission_operation")
    original = console.root
    console.root = tmp_path / "other-repository"
    shutil.copytree(original / ".devflow", console.root / ".devflow")
    (console.root / ".devflow/repository.toml").write_text(
        'schema_version=1\n[repository]\nid="other-repository"\ndefault_branch="main"\n'
    )
    console.call("work.reconcile", {
        "operation_id": "wrong-repository", "work_id": console.contract["work_id"],
        "expected_revision": console.revision,
    }, error="admission_binding")
    assert not console.counter.exists()


def test_console_records_changed_scope_as_new_admission(console):
    original = console.call("work.ready", ready_request(console.contract))
    updated = deepcopy(console.contract)
    updated["scope_revision"] = 2
    updated["scope"]["paths"].append("unrelated.txt")
    request = ready_request(updated, operation_id="amend", expected_revision=console.revision)
    changed = console.call("work.amend", request)
    assert changed["admission_id"] != original["admission_id"]
    state = console.show()
    assert "intake_admission:" + original["admission_id"] in state["records"]
    assert state["records"]["intake_admission:" + changed["admission_id"]]["user_request"] == user_request()


def test_doctor_distinguishes_missing_tools_from_authorization(console):
    doctor = console.call("doctor", env={"PATH": ""})
    assert doctor["status"] == "BLOCKED"
    assert doctor["profile"] == "valid"
    assert "git" in doctor["missing_tools"]
    assert doctor["execution_admission"] == "direct_user_request"
    assert doctor["execution_enabled"] is False
    assert not console.state_dir.exists()


def test_console_historical_pin_cannot_resume_without_recorded_request(console):
    console.start()
    state = console.show()
    # Model a pre-request active record with an old immutable policy snapshot.
    # This fixture changes only its temporary database, never production code.
    old_id = state.pop("admission_id")
    state["records"].pop("intake_admission:" + old_id)
    state["records"]["workflow_snapshot:console-snapshot"].update(
        package_version="0.2.0", package_revision="a" * 40,
    )
    with sqlite3.connect(console.state_dir / "state.sqlite3") as db:
        db.execute("UPDATE works SET state=? WHERE work_id=?", (json.dumps(state), state["work_id"]))
    console.call("work.reconcile", {
        "operation_id": "resume-old-pin", "work_id": state["work_id"],
        "expected_revision": state["revision"],
    }, error="user_request_required")
    current = console.show()
    assert current["revision"] == state["revision"]
    assert current["records"]["workflow_snapshot:console-snapshot"]["package_version"] == "0.2.0"
    assert console.call("next", {"work_id": state["work_id"]})["actions"] == [
        {"kind": "request_user_action", "reason": "user_request_required"},
    ]


def legacy_active_snapshot(console):
    """Seed historical policy bytes and associations, then upgrade the real profile."""
    console.capture_candidate()
    checked = console.call("check.run", {
        "operation_id": "historical-check", "work_id": console.contract["work_id"],
        "expected_revision": console.revision, "recipe_id": "prose", "acceptance_ids": ["A01"],
    })
    assert checked["evidence"]["execution_status"] == "PASS"
    pending = console.mutate("action.prepare", operation="prepare_workspace")["action"]
    state = console.show()
    lock = console.root / ".devflow/workflow.lock"
    current_lock = lock.read_text()
    lock.write_text('schema_version=1\nversion="0.2.0"\nrevision="' + "a" * 40 + '"\n')
    historical_profile = load_profile(console.root)
    store = SQLiteStore(console.state_dir)
    old = state["records"]["workflow_snapshot:console-snapshot"]
    sources = [
        {"reference": str(console.root / source["reference"]),
         "hash": store.put_artifact((console.root / source["reference"]).read_bytes())}
        for source in historical_profile.sources
    ]
    old.update(
        package_version="0.2.0", package_revision="a" * 40,
        repository_profile_reference="sha256:" + historical_profile.fingerprint,
        instruction_sources=sources,
        workflow_hash=profile_digest({
            "package_version": "0.2.0", "release": historical_profile.lock,
            "instructions": sources, "profile": historical_profile.fingerprint,
        }),
    )
    old_admission = state.pop("admission_id")
    state["records"].pop("intake_admission:" + old_admission)
    # This writes only fixture history; production transitions must preserve it.
    with sqlite3.connect(console.state_dir / "state.sqlite3") as db:
        db.execute("UPDATE works SET state=? WHERE work_id=?", (json.dumps(state), state["work_id"]))
        db.execute("UPDATE records SET payload=? WHERE work_id=? AND record_key=?", (
            canonical_json(old), state["work_id"], "workflow_snapshot:console-snapshot",
        ))
        db.execute("DELETE FROM records WHERE work_id=? AND record_key=?", (
            state["work_id"], "intake_admission:" + old_admission,
        ))
    lock.write_text(current_lock)
    assert load_profile(console.root).fingerprint != historical_profile.fingerprint
    return deepcopy(state), pending


def fresh_snapshot(console):
    return console.call("snapshot.capture", {
        "snapshot_id": "renewed-snapshot", "effective_settings": {
            "model": "synthetic-current-model", "reasoning_effort": "synthetic-current-effort",
            "service_tier": "synthetic-tier", "source_reference": "synthetic:current-settings",
        },
    })


def test_legacy_active_amendment_renews_validated_policy_and_checks_once(console):
    historical, pending = legacy_active_snapshot(console)
    c = deepcopy(console.contract)
    c["scope_revision"] = 2
    request = ready_request(c, operation_id="renew-policy", expected_revision=console.revision)
    console.call("work.amend", request, error="workflow_snapshot_required")
    snapshot = fresh_snapshot(console)
    request["workflow_snapshot"] = snapshot
    amended = console.call("work.amend", request)
    state = console.show()
    assert state["attempt"]["workflow_snapshot_id"] == "renewed-snapshot"
    assert state["attempt"]["model_policy_snapshot_id"] == "renewed-snapshot"
    assert state["attempt"]["attempt_id"] == historical["attempt"]["attempt_id"]
    assert state["candidate_id"] is None and state["check_ids"] == {} and state["gate_ids"] == {}
    assert state["actions"][pending["action_id"]]["status"] == "invalidated"
    assert all(state["records"][key] == value for key, value in historical["records"].items())
    assert state["records"]["workflow_snapshot:renewed-snapshot"] == snapshot
    console.mutate(
        "candidate.capture", candidate_id="renewed-candidate", base_ref=console.base,
        dependency_hash=digest("fixture dependencies"), environment_hash=digest(sys.version),
        ownership_token="synthetic-console-owner",
    )
    check_request = {
        "operation_id": "renewed-check", "work_id": console.contract["work_id"],
        "expected_revision": console.revision, "recipe_id": "prose", "acceptance_ids": ["A01"],
    }
    checked = console.call("check.run", check_request)
    assert checked["evidence"]["execution_status"] == "PASS"
    assert checked["evidence"]["candidate_id"] == "renewed-candidate"
    assert console.counter.read_text() == "run\nrun\n"
    final_revision = console.revision
    assert console.call("work.amend", request) == amended
    assert console.call("check.run", check_request) == checked
    assert console.counter.read_text() == "run\nrun\n"
    assert console.show()["revision"] == final_revision


@pytest.mark.parametrize("broken,error", [
    ("malformed", "invalid_record"), ("revision", "release_mismatch"),
    ("profile", "profile_drift"), ("artifact", "missing_artifact"),
    ("settings", "settings_mismatch"), ("workflow", "snapshot_mismatch"),
    ("reuse_id", "immutable_record"),
])
def test_active_amendment_cannot_adopt_unvalidated_snapshot(console, broken, error):
    console.start()
    historical = console.show()
    snapshot = fresh_snapshot(console)
    if broken == "malformed":
        snapshot = {"snapshot_id": "invalid"}
    elif broken == "revision":
        snapshot["package_revision"] = "a" * 40
    elif broken == "profile":
        snapshot["repository_profile_reference"] = "sha256:" + "a" * 64
    elif broken == "artifact":
        snapshot["instruction_sources"][0]["hash"] = "f" * 64
    elif broken == "settings":
        snapshot["effective_settings_reference"] = "synthetic:uncaptured-settings"
    elif broken == "reuse_id":
        snapshot["snapshot_id"] = "console-snapshot"
    else:
        snapshot["workflow_hash"] = "f" * 64
    c = deepcopy(console.contract)
    c["scope_revision"] = 2
    console.call("work.amend", ready_request(
        c, operation_id="invalid-renewal", expected_revision=console.revision,
        workflow_snapshot=snapshot,
    ), error=error)
    assert console.show() == historical


def test_current_active_amendment_can_retain_its_valid_snapshot(console):
    console.start()
    historical = console.show()
    c = deepcopy(console.contract)
    c["scope_revision"] = 2
    console.mutate("work.amend", record=c, user_request=user_request())
    state = console.show()
    assert state["attempt"]["workflow_snapshot_id"] == "console-snapshot"
    assert state["attempt"]["model_policy_snapshot_id"] == "console-snapshot"
    assert state["records"]["workflow_snapshot:console-snapshot"] == historical["records"]["workflow_snapshot:console-snapshot"]


@pytest.mark.parametrize("github", [False, True])
def test_doctor_separates_local_readiness_and_missing_optional_capabilities(console, tmp_path, github):
    if github:
        profile = console.root / ".devflow/repository.toml"
        profile.write_text(profile.read_text().replace(console.identity, "github:synthetic/repo"))
    tools = tmp_path / "available-tools"
    tools.mkdir()
    (tools / "git").symlink_to(shutil.which("git"))
    doctor = console.call("doctor", env={"PATH": str(tools)})
    assert doctor["status"] == "READY" and doctor["execution_enabled"]
    capabilities = doctor["capabilities"]
    assert capabilities["local_execution"]["ready"]
    assert capabilities["github_capture"]["applicable"] is github
    assert capabilities["github_capture"]["ready"] is False
    assert capabilities["github_capture"]["required_tools"] == ["git", "gh"]
    assert capabilities["github_capture"]["missing_tools"] == ["gh"]
    assert capabilities["managed_launcher"]["missing_tools"] == ["uv"]
    assert capabilities["usage_collection"]["missing_tools"] == ["npx"]
