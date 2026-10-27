"""Exercise future-file gate admission and actual per-source test reports."""

from __future__ import annotations

import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from copy import deepcopy
from types import SimpleNamespace

import pytest

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_feature_execution import worker_spec
from devflow_temporal.delivery_feature_gates import (
    GateAdmissionError,
    derive_chunk_gates,
    resolve_required_selectors,
    selector_evidence,
    validate_chunk_gates,
)
from devflow_temporal.delivery_github_contract import ordered_chunks

BULK = "apps/web/e2e/tests/jobs-bulk.spec.ts"
PAGINATION = "apps/web/e2e/tests/job-list-pagination.spec.ts"


def git(checkout, *args):
    return subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "-C", str(checkout), *args],
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()


def selection(selectors):
    return {"stage": "browser_qa", "recipe_id": "job-list-browser", "selectors": selectors}


@pytest.fixture
def gate_project(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-q")
    git(source, "config", "user.name", "Fixture")
    git(source, "config", "user.email", "fixture@example.invalid")
    for relative, content in {
        "README.md": "Feature API",
        "apps/web/package.json": '{"name":"@jobctrl/web"}',
        "apps/web/e2e/playwright.config.ts": "export default {};",
        BULK: "test('existing browser',()=>{});",
    }.items():
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    git(source, "add", ".")
    git(source, "commit", "-qm", "Frozen browser baseline")

    def chunk(ident, dependencies, paths, selectors):
        return {
            "id": ident,
            "title": ident,
            "scope": "Complete " + ident,
            "steps": ["Implement a complete layer"],
            "verification": ["Run product checks"],
            "acceptance": ["Works with current browser"],
            "expected_paths": paths,
            "depends_on": dependencies,
            "gates": [selection(selectors)],
        }

    plan = {
        "version": 2,
        "scope": "Combined pagination",
        "acceptance": ["Bounded browser requests"],
        "workstreams": [
            {
                "id": "api",
                "title": "API",
                "issue_number": 10,
                "acceptance": ["Compatible"],
                "chunks": [chunk("api1", [], ["README.md"], [BULK])],
            },
            {
                "id": "web",
                "title": "Web",
                "issue_number": 11,
                "acceptance": ["One request"],
                "chunks": [chunk("web1", ["api1"], [PAGINATION], [BULK, PAGINATION])],
            },
        ],
        "final_gates": [selection([BULK, PAGINATION])],
    }
    policy = {
        "allowed_paths": ["README.md", BULK, PAGINATION],
        "checks": [],
        "prepublish_checks": [],
        "browser_qa": {
            "id": "job-list-browser",
            "cwd": ".",
            "argv": [
                "corepack",
                "pnpm",
                "--filter",
                "@jobctrl/web",
                "exec",
                "playwright",
                "test",
                "--config",
                "e2e/playwright.config.ts",
                "e2e/tests/jobs-bulk.spec.ts",
                "e2e/tests/job-list-pagination.spec.ts",
            ],
            "min_tests": 1,
            "ports": {"QA_API_PORT": 18971, "QA_WEB_PORT": 18972},
            "test_count_regex": r"(\d+) passed",
            "timeout_seconds": 60,
            "artifact_paths": [],
        },
    }
    spec = {
        "run_id": "parent",
        "source_path": str(source),
        "base_sha": git(source, "rev-parse", "HEAD"),
        "policy": policy,
        "policy_digest": digest(policy),
        "accepted_plan": json.dumps(plan),
        "request_digest": "parent-input",
        "issue_url": "https://github.com/example/fixture/issues/3",
        "state_dir": str(tmp_path / "runs/parent"),
        "checkout": str(tmp_path / "checkouts/parent"),
        "feature_delivery": {
            "registry": str(tmp_path / "registry.sqlite3"),
            "owner": {"issue_id": "I_parent", "generation": 1},
        },
    }
    return spec, plan, source


def test_api_chunk_proves_current_browser_and_final_keeps_pagination(gate_project):
    from devflow_temporal.role_runner import _task

    spec, plan, source = gate_project
    reason = (
        "Verify rendered current-browser navigation, filters and bulk operations on api1; "
        "the pagination spec is created by dependent web1."
    )
    plan["workstreams"][0]["chunks"][0]["gates"][0]["reason"] = reason
    spec["accepted_plan"] = json.dumps(plan)
    original = deepcopy(spec)
    chunks = ordered_chunks(plan)
    api = worker_spec(
        spec,
        chunks[0],
        {"url": "https://github.com/example/fixture/issues/10"},
        kind="build",
        base_sha=spec["base_sha"],
        base_branch="main",
    )
    qa = api["policy"]["browser_qa"]
    assert qa["required_selectors"] == [BULK]
    assert "e2e/tests/jobs-bulk.spec.ts" in qa["argv"]
    assert "e2e/tests/job-list-pagination.spec.ts" not in qa["argv"]
    assert "--config" in qa["argv"] and "e2e/playwright.config.ts" in qa["argv"]
    assert qa["ports"] == original["policy"]["browser_qa"]["ports"]
    assert api["policy"]["allowed_paths"] == original["policy"]["allowed_paths"]
    assert api["expected_paths"] == ["README.md"]
    assert resolve_required_selectors(qa, source)[0]["selector"] == BULK
    api["provider"] = "fake"
    api["policy"]["roles"] = {
        role: {"model": "fixture-model", "effort": "high"}
        for role in ("implement", "review", "verify")
    }
    for role in ("implement", "review", "verify"):
        task = _task({
            "spec": api,
            "role": role,
            "candidate": {"id": "api-candidate", "head": spec["base_sha"]},
            "workspace": str(source),
            "iteration": 1,
        })
        assert json.dumps(api["feature_gate_selections"], sort_keys=True) in task.goal
        assert reason in task.goal
    final = derive_chunk_gates(spec, plan, chunks[-1], final=True)
    assert final["policy"]["browser_qa"]["required_selectors"] == [BULK, PAGINATION]
    with pytest.raises(GateAdmissionError, match="fixed tracked candidate"):
        resolve_required_selectors(final["policy"]["browser_qa"], source)
    assert spec == original


@pytest.mark.parametrize("bad", ["dropped_final", "outside_authority"])
def test_plan_cannot_weaken_final_gates_or_expand_authority(gate_project, bad):
    spec, plan, _ = gate_project
    if bad == "dropped_final":
        plan["final_gates"][0]["selectors"] = [BULK]
    elif bad == "unadmitted":
        plan["workstreams"][0]["chunks"][0]["gates"][0]["recipe_id"] = "free-command"
    elif bad == "duplicate":
        plan["final_gates"][0]["selectors"] = [BULK, BULK, PAGINATION]
    else:
        plan["workstreams"][0]["chunks"][0]["expected_paths"].append("unapproved.py")
    with pytest.raises(ValueError):
        validate_chunk_gates(plan, spec)


@pytest.mark.parametrize("project_name", [".", "workers/automation"])
@pytest.mark.parametrize("family", ["xunit1", "xunit2"])
def test_real_pytest_junit_proves_each_selected_source_and_rejects_other_file(
    tmp_path, monkeypatch, project_name, family,
):
    monkeypatch.setenv("PYTEST_DISABLE_PLUGIN_AUTOLOAD", "1")
    monkeypatch.delenv("PYTEST_ADDOPTS", raising=False)
    monkeypatch.delenv("PYTEST_PLUGINS", raising=False)
    source = tmp_path / "source"
    project = source / project_name
    tests = project / "tests"
    tests.mkdir(parents=True)
    config = project / "pyproject.toml"
    config.write_text('[tool.pytest.ini_options]\njunit_family = "' + family + '"\n')
    content = (
        "from pathlib import Path\n"
        "def test_actual(pytestconfig):\n"
        "    assert pytestconfig.rootpath == Path(__file__).resolve().parents[1]\n"
        "class TestOuter:\n"
        "    def test_method(self): assert True\n"
        "    class TestNested:\n"
        "        def test_nested(self): assert True\n"
    )
    for name in ("test_present.py", "test_other.py"):
        (tests / name).write_text(content)
    git(source, "init", "-q")
    git(source, "config", "user.name", "Fixture")
    git(source, "config", "user.email", "fixture@example.invalid")
    git(source, "add", ".")
    git(source, "commit", "-qm", "Frozen pytest configuration")
    selectors = [(tests / name).relative_to(source).as_posix()
                 for name in ("test_present.py", "test_other.py")]
    recipe = {
        "cwd": ".",
        "argv": ["uv", "--project", project_name, "run", "--locked", "--all-extras",
                 "pytest", *selectors, "-q", "--junitxml={report_path}"],
        "required_selectors": selectors,
        "selector_evidence_version": 1,
        "selector_project": ".",
    }
    spec = {"source_path": str(source), "base_sha": git(source, "rev-parse", "HEAD")}
    sources = resolve_required_selectors(recipe, source)
    report = tmp_path / "actual-junit.xml"

    def run(selected, *, success=True):
        process = subprocess.run(
            [sys.executable, "-m", "pytest", *selected, "-q", "--junitxml=" + str(report)],
            cwd=source, capture_output=True,
        )
        assert process.returncode == (0 if success else 1), process.stdout + process.stderr
        return report.read_bytes()

    partial = run(selectors[:1])
    cases = ET.fromstring(partial).findall(".//testcase")
    assert {case.get("classname") for case in cases} == {
        "tests.test_present", "tests.test_present.TestOuter",
        "tests.test_present.TestOuter.TestNested",
    }
    assert all(case.get("file") == ("tests/test_present.py" if family == "xunit1" else None)
               for case in cases)
    with pytest.raises(ValueError, match="test_other.py"):
        selector_evidence(recipe, source, sources, junit=partial, spec=spec)
    complete = run(selectors)
    receipts = selector_evidence(recipe, source, sources, junit=complete, spec=spec)
    assert {item["selector"]: item["passed"] for item in receipts} == dict.fromkeys(selectors, 3)

    foreign = ET.fromstring(complete)
    for case in foreign.findall(".//testcase"):
        if case.get("classname", "").startswith("tests.test_present"):
            case.set("file", "foreign/tests/test_present.py")
    with pytest.raises(ValueError, match="test_present.py"):
        selector_evidence(recipe, source, sources, junit=ET.tostring(foreign), spec=spec)

    # Only the unchanged admitted configuration can supply an extra report root.
    if project_name != ".":
        original = config.read_text()
        config.write_text(original + 'addopts = "--rootdir=."\n')
        with pytest.raises(ValueError, match="no complete passing evidence"):
            selector_evidence(recipe, source, sources, junit=complete, spec=spec)
        config.write_text(original)
        shadow = tests / "pytest.ini"
        shadow.write_text("[pytest]\n")
        with pytest.raises(ValueError, match="no complete passing evidence"):
            selector_evidence(recipe, source, sources, junit=complete, spec=spec)
        shadow.unlink()
        for option in ("--rootdir=.", "--junitprefix=tests.test_present"):
            unsupported = {**recipe, "argv": [*recipe["argv"], option]}
            with pytest.raises(ValueError, match="no complete passing evidence"):
                selector_evidence(unsupported, source, sources, junit=complete, spec=spec)

        ambiguous = [project / "tests.more/test_present.py",
                     project / "tests/more/test_present.py"]
        for path in ambiguous:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("def test_ambiguous(): assert True\n")
        git(source, "add", ".")
        paths = [path.relative_to(source).as_posix() for path in ambiguous]
        collision = {**recipe, "required_selectors": paths,
                     "argv": ["pytest", *paths, "-q"]}
        diagnostic = "ambiguous" if family == "xunit2" else "no complete passing evidence"
        with pytest.raises(ValueError, match=diagnostic):
            selector_evidence(collision, source, resolve_required_selectors(collision, source),
                              junit=run(paths[:1]), spec=spec)

    # Actual failing cases retain failure receipts instead of becoming attribution errors.
    (tests / "test_present.py").write_text(content.replace("assert True", "assert False", 1))
    sources = resolve_required_selectors(recipe, source)
    with pytest.raises(ValueError, match="test_present.py") as raised:
        selector_evidence(recipe, source, sources, junit=run(selectors, success=False), spec=spec)
    failed = raised.value._failed_selector_receipts
    assert next(item for item in failed if item["selector"] == selectors[0])["failed"] == 1


def test_new_unstaged_test_passes_real_broker_prechecks_before_publication(tmp_path):
    from devflow_temporal.delivery_broker import DeliveryBroker

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    git(checkout, "init", "-q")
    git(checkout, "config", "user.name", "Fixture")
    git(checkout, "config", "user.email", "fixture@example.invalid")
    (checkout / "README.md").write_text("Baseline")
    git(checkout, "add", ".")
    git(checkout, "commit", "-qm", "Frozen source")
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    recipe = {
        "id": "local-pytest",
        "kind": "test",
        "cwd": ".",
        "argv": [sys.executable, "-m", "pytest", "tests/test_new.py", "-q"],
        "timeout_seconds": 30,
        "test_count_regex": r"(\d+) passed",
        "min_tests": 1,
        "required_selectors": ["tests/test_new.py"],
        "selector_evidence_version": 1,
        "selector_project": ".",
    }
    policy = {
        "allowed_paths": ["tests/test_new.py"],
        "host_sandbox": "trusted-local",
        "prepublish_checks": [recipe],
    }
    broker = object.__new__(DeliveryBroker)
    broker.checkout, broker.state_dir, broker.evidence_dir = checkout, state, state / "evidence"
    broker.spec = {
        "provider": "fake",
        "run_id": "worker",
        "gate_selections_version": 2,
        "base_sha": git(checkout, "rev-parse", "HEAD"),
        "policy": policy,
        "policy_digest": digest(policy),
        "expected_paths": ["README.md"],
    }
    initial = broker.candidate()
    (checkout / "tests").mkdir()
    (checkout / "tests/test_new.py").write_text("def test_new(): assert 2 + 2 == 4\n")
    candidate = broker.admit_implementation(initial)
    assert candidate["head"] == initial["head"] and candidate["id"] != initial["id"]
    assert not git(checkout, "ls-files", "--", "tests/test_new.py")
    result = broker.run_prechecks(0, candidate)
    assert result["state"] == "passed", result
    receipt = result["results"][0]
    assert receipt["selectors"][0]["selector"] == "tests/test_new.py"
    assert receipt["selectors"][0]["passed"] == 1
    assert receipt["artifacts"]["candidate_id"] == candidate["id"]
    assert result["source_unchanged"] and broker.candidate() == candidate
    assert not git(checkout, "diff", "--cached", "--name-only")


@pytest.fixture
def native_intake_provider(tmp_path, monkeypatch):
    """Deterministic provider completion through the actual native role normalizer."""
    import hashlib

    from agent_runtime_kit import AgentResult, Usage

    from devflow_temporal import role_runner

    binary = tmp_path / "unused-provider"
    binary.write_bytes(b"fixture provider; never executed")
    monkeypatch.setattr(role_runner, "_task", lambda _request: SimpleNamespace(
        model="fixture-native", reasoning_effort="low"))
    monkeypatch.setattr("agent_runtime_kit.validate_task",
                        lambda *_args: SimpleNamespace(supported=True))

    async def normalize(request, output, *, provider_failure=False, observation_state="confirmed"):
        async def complete(_task):
            return AgentResult(
                output=json.dumps(output), parsed_output=deepcopy(output),
                finish_reason="failed" if provider_failure else "done",
                error="Controlled provider failure" if provider_failure else None,
                session_id=f"native-intake:{request['iteration']}", usage=Usage(total_tokens=13),
            )

        monkeypatch.setattr("agent_runtime_kit.adapters.CodexAgentRuntime",
                            lambda **_kwargs: SimpleNamespace(run=complete))
        monkeypatch.setattr("devflow_temporal.delivery_native_threads.NativeThreadObservation",
                            lambda _request: SimpleNamespace(
                                legacy=True, data={"state": observation_state},
                                codex_class=lambda: object, failure=lambda: None,
                                reference=lambda: {"state": observation_state},
                            ))
        native_request = deepcopy(request)
        native_request["spec"]["policy"].update(
            codex_bin=str(binary), codex_bin_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            config_overrides=[], execution_backend="native-macos", host_sandbox="native-profile",
        )
        return await role_runner._run_codex(native_request)

    return normalize
