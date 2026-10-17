"""Exercise future-file gate admission and actual per-source test reports."""

from __future__ import annotations

import json
import subprocess
import sys
import xml.etree.ElementTree as ET
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_feature_execution import plan_for, revised_worker_spec, worker_spec
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


def test_future_selector_cannot_be_required_before_its_owner(gate_project):
    spec, plan, _ = gate_project
    plan["workstreams"][0]["chunks"][0]["gates"] = [selection([BULK, PAGINATION])]
    with pytest.raises(GateAdmissionError, match="outside this chunk's prerequisites") as raised:
        validate_chunk_gates(plan, spec)
    assert raised.value.diagnostic["chunk_id"] == "api1"
    assert raised.value.diagnostic["selector"] == PAGINATION


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


def test_legacy_plan_keeps_original_exact_allowlist_and_browser_recipe(gate_project):
    spec, plan, _ = gate_project
    plan.pop("version")
    plan.pop("final_gates")
    for stream in plan["workstreams"]:
        for chunk in stream["chunks"]:
            chunk["allowed_paths"] = chunk.pop("expected_paths")
            chunk.pop("gates")
    spec["accepted_plan"] = json.dumps(plan)
    chunk = ordered_chunks(plan_for(spec))[0]
    worker = worker_spec(
        spec,
        chunk,
        {"url": "https://github.com/example/fixture/issues/10"},
        kind="build",
        base_sha=spec["base_sha"],
        base_branch="main",
    )
    assert worker["policy"]["allowed_paths"] == ["README.md"]
    assert worker["policy"]["browser_qa"] == spec["policy"]["browser_qa"]
    assert "gate_selections_version" not in worker


def test_revision_overlay_preserves_worker_and_session_identity(gate_project):
    spec, plan, _ = gate_project
    chunk = ordered_chunks(plan)[0]
    worker = worker_spec(
        spec,
        chunk,
        {"url": "https://github.com/example/fixture/issues/10"},
        kind="build",
        base_sha=spec["base_sha"],
        base_branch="main",
    )
    worker["continuation"] = {
        "session_id": "original-session",
        "candidate_id": "original-candidate",
    }
    original = deepcopy(worker)
    plan["workstreams"][0]["chunks"][0]["steps"].append("Clarify a demonstrated planning defect")
    identity = {"plan_revision": 2, "plan_digest": digest(plan)}
    revised = revised_worker_spec(spec, plan, "api1", expected_revision=identity, worker=worker)
    for field in (
        "run_id",
        "work_id",
        "command_id",
        "branch",
        "checkout",
        "state_dir",
        "base_sha",
        "feature_delivery",
        "feature_worker",
        "continuation",
        "request_digest",
    ):
        assert revised[field] == original[field]
    assert revised["feature_plan_revision"] == identity
    assert revised["policy"]["allowed_paths"] == spec["policy"]["allowed_paths"]
    assert revised["policy_digest"] == digest(revised["policy"])
    assert worker == original


def playwright_report(source, paths):
    return "1 passed\n" + json.dumps(
        {
            "config": {"rootDir": str(source / "apps/web/e2e/tests")},
            "stats": {"expected": len(paths), "unexpected": 0, "skipped": 0, "flaky": 0},
            "suites": [
                {
                    "file": Path(path).name,
                    "specs": [
                        {
                            "file": Path(path).name,
                            "tests": [
                                {
                                    "expectedStatus": "passed",
                                    "status": "expected",
                                    "results": [{"status": "passed"}],
                                }
                            ],
                        }
                    ],
                }
                for path in paths
            ],
        }
    )


def test_mixed_playwright_report_cannot_qualify_an_unexecuted_required_file(gate_project):
    spec, plan, source = gate_project
    (source / PAGINATION).write_text("test('final pagination',()=>{});")
    git(source, "add", PAGINATION)
    qa = derive_chunk_gates(spec, plan, ordered_chunks(plan)[-1], final=True)["policy"][
        "browser_qa"
    ]
    sources = resolve_required_selectors(qa, source)
    with pytest.raises(ValueError, match="no complete passing evidence"):
        selector_evidence(qa, source, sources, playwright=playwright_report(source, [BULK]))
    receipts = selector_evidence(
        qa, source, sources, playwright=playwright_report(source, [BULK, PAGINATION])
    )
    assert {r["selector"] for r in receipts} == {BULK, PAGINATION}
    assert all(r["passed"] == 1 and not r["failed"] and not r["skipped"] for r in receipts)


@pytest.mark.parametrize("bad", ["untracked", "duplicate"])
def test_named_selector_rejects_untracked_links_and_duplicate_paths(gate_project, bad):
    spec, plan, source = gate_project
    qa = derive_chunk_gates(spec, plan, ordered_chunks(plan)[0])["policy"]["browser_qa"]
    if bad == "untracked":
        git(source, "rm", "--cached", BULK)
    elif bad == "symlink":
        (source / BULK).unlink()
        (source / BULK).symlink_to(source / "README.md")
    else:
        qa["required_selectors"].append(BULK)
    with pytest.raises(GateAdmissionError):
        resolve_required_selectors(qa, source)


def test_browser_entrypoint_rejects_future_file_before_creating_effect(gate_project, monkeypatch):
    import devflow_temporal.delivery_native_guard as guard
    import devflow_temporal.delivery_preparation as preparation
    from devflow_temporal.delivery_browser_qa import run_browser_qa

    spec, plan, source = gate_project
    spec.update(derive_chunk_gates(spec, plan, ordered_chunks(plan)[-1], final=True))
    spec["policy_digest"] = digest(spec["policy"])
    monkeypatch.setattr(guard, "validate_native_turn", lambda *_: None)
    monkeypatch.setattr(preparation, "verify_prepared_spec", lambda *_: None)
    candidate = {"id": "candidate"}
    broker = SimpleNamespace(
        spec=spec,
        store=None,
        effect_namespace="",
        evidence_dir=source.parent / "evidence",
        candidate=lambda: candidate,
        gate_checkout=lambda *_: source,
    )
    result = run_browser_qa(broker, 0, candidate)
    assert result["state"] == "failed" and result["cleanup"] == "confirmed"
    assert result["launched"] is False
    assert Path(result["selector_admission"]["path"]).is_file()


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


@pytest.mark.parametrize("failure", [None, "candidate_excluded"])
def test_v2_new_selector_requires_nonignored_candidate_source_authority(gate_project, failure):
    spec, plan, source = gate_project
    spec.update(derive_chunk_gates(spec, plan, ordered_chunks(plan)[-1], final=True))
    (source / PAGINATION).write_text("test('new pagination',()=>{});")
    if failure == "ignored":
        (source / ".gitignore").write_text(PAGINATION + "\n")
    elif failure == "outside_authority":
        spec["policy"]["allowed_paths"].remove(PAGINATION)
    elif failure == "candidate_excluded":
        excluded = "node_modules/future.spec.ts"
        (source / excluded).parent.mkdir()
        (source / excluded).write_text("test('dependency',()=>{});")
        spec["policy"]["allowed_paths"].append(excluded)
        spec["policy"]["browser_qa"]["required_selectors"] = [excluded]
    qa = spec["policy"]["browser_qa"]
    assert not git(source, "ls-files", "--", PAGINATION)
    if failure:
        with pytest.raises(GateAdmissionError):
            resolve_required_selectors(qa, source, spec=spec)
    else:
        assert {s["selector"] for s in resolve_required_selectors(qa, source, spec=spec)} == {
            BULK,
            PAGINATION,
        }
        with pytest.raises(GateAdmissionError):
            resolve_required_selectors(qa, source)


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


def test_structured_tracked_recipe_focuses_runner_without_prose_inference(gate_project):
    from devflow_temporal.delivery_plan_checks import planned_checks

    spec, plan, source = gate_project
    test = "apps/web/src/list.test.ts"
    (source / test).parent.mkdir(parents=True)
    (source / test).write_text("test('actual current list',()=>{});")
    (source / "scripts").mkdir()
    (source / "scripts/checks.toml").write_text("""schema_version = 1
[checks.web]
kind = "junit"
cwd = "."
argv = ["corepack", "pnpm", "--filter", "@jobctrl/web", "exec", "vitest", "run",
        "--reporter=junit", "--outputFile={report_path}"]
timeout_seconds = 60
""")
    git(source, "add", ".")
    git(source, "commit", "-qm", "Frozen tracked recipe")
    spec["base_sha"] = git(source, "rev-parse", "HEAD")
    gate = {"stage": "checks", "recipe_id": "checks.web", "selectors": [test]}
    plan["workstreams"][0]["chunks"][0]["gates"].append(gate)
    plan["final_gates"].append(gate)
    plan["workstreams"][0]["chunks"][0]["verification"] = [
        "Run the selected list test; future_prose_only.test.ts is planned later"
    ]
    spec["accepted_plan"] = json.dumps(plan)
    chunk = ordered_chunks(plan)[0]
    worker = worker_spec(
        spec,
        chunk,
        {"url": "https://github.com/example/fixture/issues/10"},
        kind="build",
        base_sha=spec["base_sha"],
        base_branch="main",
    )
    checks = planned_checks(worker, source, source.parent / "evidence")
    assert len(checks) == 1
    check = checks[0]
    assert check["required_selectors"] == [test]
    runner = check["argv"].index("vitest")
    assert check["argv"][runner : runner + 3] == ["vitest", "run", "src/list.test.ts"]
    assert "future_prose_only" not in " ".join(check["argv"])
    assert check["junit_required"]


def test_gate_admission_rejects_selectors_outside_recipe_package(gate_project):
    spec, plan, _ = gate_project
    plan["workstreams"][0]["chunks"][0]["gates"] = [selection(["other/list.spec.ts"])]
    with pytest.raises(GateAdmissionError, match="escaped the admitted recipe package"):
        validate_chunk_gates(plan, spec)


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("executable", ["node", "/fixed/bin/node"])
def test_node_named_selector_is_rejected_during_plan_admission(gate_project, tracked, executable):
    spec, plan, source = gate_project
    test = "tests/actual.test.mjs"
    (source / test).parent.mkdir()
    (source / test).write_text("import test from 'node:test'; test('actual case',()=>{});\n")
    node = {
        "id": "node-test",
        "kind": "test",
        "cwd": ".",
        "argv": [executable, "--test", test],
        "timeout_seconds": 30,
    }
    if tracked:
        (source / "scripts").mkdir()
        (source / "scripts/checks.toml").write_text("""schema_version = 1
[checks.node]
kind = "junit"
cwd = "."
argv = ["node", "--test", "--test-reporter=junit",
        "--test-reporter-destination={report_path}", "tests/actual.test.mjs"]
timeout_seconds = 30
""")
        metadata = source / "scripts/checks.toml"
        metadata.write_text(
            metadata.read_text().replace(
                '["node", "--test",', "[" + json.dumps(executable) + ', "--test",'
            )
        )
        recipe_id = "checks.node"
    else:
        spec["policy"]["checks"] = [node]
        recipe_id = "node-test"
    git(source, "add", ".")
    git(source, "commit", "-qm", "Frozen Node recipe")
    spec["base_sha"] = git(source, "rev-parse", "HEAD")
    gate = {"stage": "checks", "recipe_id": recipe_id, "selectors": [test]}
    plan["workstreams"][0]["chunks"][0]["gates"].append(gate)
    plan["final_gates"].append(gate)
    with pytest.raises(GateAdmissionError, match="lack admitted per-file report provenance"):
        validate_chunk_gates(plan, spec)
    # Historical plans retain the original recipe and exact allowlist behavior.
    legacy = deepcopy(plan)
    legacy.pop("version")
    legacy.pop("final_gates")
    for stream in legacy["workstreams"]:
        for chunk in stream["chunks"]:
            chunk["allowed_paths"] = chunk.pop("expected_paths")
            chunk.pop("gates")
    assert derive_chunk_gates(spec, legacy, ordered_chunks(legacy)[0])["policy"] == spec["policy"]


@pytest.mark.parametrize("junit_required", [False, True])
@pytest.mark.parametrize("executable", ["node", "/fixed/bin/node"])
def test_node_selector_report_binding_rejects_unattributable_junit(
    tmp_path, junit_required, executable
):
    from devflow_temporal.delivery_feature_gates import bind_selector_report

    recipe = {
        "id": "node-test",
        "kind": "test",
        "cwd": ".",
        "argv": [executable, "--test", "tests/actual.test.mjs"],
        "required_selectors": ["tests/actual.test.mjs"],
        "selector_evidence_version": 1,
        "junit_required": junit_required,
    }
    with pytest.raises(GateAdmissionError, match="lack admitted per-file report provenance"):
        bind_selector_report(recipe, tmp_path / "evidence")
    assert not (tmp_path / "evidence").exists()
    recipe.pop("required_selectors")
    assert bind_selector_report(recipe, tmp_path / "evidence") is recipe


@pytest.mark.parametrize(("bad", "detail"), [
    ("directory", "concrete supported test file"),
    ("config", "concrete supported test file"),
    ("lock", "concrete supported test file"),
    ("package_relative", "escaped the admitted recipe package"),
    ("unknown_runner", "supported runner or owned JUnit"),
    ("node", "lack admitted per-file report provenance"),
    ("missing_browser", "current compatibility selectors"),
    ("missing_final", "dropped admitted required selectors"),
    ("future", "outside this chunk's prerequisites"),
])
async def test_intake_reports_gate_errors_without_rewriting_provider_plan(
    gate_project, monkeypatch, bad, detail,
):
    from devflow_temporal import delivery_activities as activities

    spec, plan, source = gate_project
    spec["provider"] = "fake"
    spec["feature_plan_version"] = 2
    chunk = plan["workstreams"][0]["chunks"][0]
    selectors = {
        "directory": "packages/contracts",
        "config": "apps/web/tsconfig.json",
        "lock": "pnpm-lock.yaml",
        "package_relative": "e2e/tests/jobs-bulk.spec.ts",
    }
    if bad in selectors:
        chunk["gates"][0]["selectors"] = [selectors[bad]]
    elif bad in {"unknown_runner", "node"}:
        spec["policy"]["checks"] = [{
            "id": "lint-test", "cwd": ".",
            "argv": ["node", "--test", BULK] if bad == "node" else ["node", "scripts/lint.mjs"],
        }]
        chunk["gates"].append({"stage": "checks", "recipe_id": "lint-test", "selectors": [BULK]})
    elif bad == "missing_browser":
        chunk["gates"] = []
    elif bad == "missing_final":
        plan["final_gates"] = []
    else:
        chunk["gates"][0]["selectors"] = [BULK, PAGINATION]
    candidate = {"id": "original", "head": spec["base_sha"]}
    broker = SimpleNamespace(checkout=source, candidate=lambda: candidate)
    native = {"status": "plan", "summary": "Original proposal", "plan": deepcopy(plan),
              "findings": [], "session_id": "original-native-session", "usage": {"total": 13}}

    async def run(_request):
        return deepcopy(native)

    monkeypatch.setattr(activities, "_context", lambda _spec: (object(), broker))
    monkeypatch.setattr(activities, "get_supervisor", lambda _store: SimpleNamespace(run=run))
    result = await activities._run_intake({"spec": spec, "candidate": candidate, "iteration": 0})
    assert all(result[key] == value for key, value in native.items())
    assert result["plan_validation"]["status"] == "invalid"
    assert detail in result["plan_validation"]["findings"][0]["detail"]
    assert result["candidate"] == candidate
    assert result["plan"] == plan


@pytest.mark.parametrize("failure", ["provider", "cleanup", "checkout", "metadata"])
async def test_intake_does_not_replan_unrelated_failures(gate_project, monkeypatch, failure):
    from devflow_temporal import delivery_activities as activities

    spec, plan, source = gate_project
    spec.update(provider="fake", feature_plan_version=2)
    plan["workstreams"][0]["chunks"][0]["gates"][0]["selectors"] = ["pnpm-lock.yaml"]
    candidate = {"id": "original", "head": spec["base_sha"]}
    observed = iter([candidate, {**candidate, "id": "changed"}])
    broker = SimpleNamespace(checkout=source, candidate=(
        lambda: next(observed) if failure == "checkout" else candidate
    ))
    native = {"status": "blocked" if failure == "provider" else "plan", "plan": plan,
              "findings": ["provider failure"] if failure == "provider" else [],
              "cleanup": "unknown" if failure == "cleanup" else "confirmed"}

    async def run(_request):
        return deepcopy(native)

    monkeypatch.setattr(activities, "_context", lambda _spec: (object(), broker))
    monkeypatch.setattr(activities, "get_supervisor", lambda _store: SimpleNamespace(run=run))
    request = {"spec": spec, "candidate": candidate, "iteration": 0}
    if failure == "metadata":
        spec["base_sha"] = "not-a-frozen-commit"
        with pytest.raises(ValueError, match="frozen source commit"):
            await activities._run_intake(request)
    else:
        result = await activities._run_intake(request)
        assert "plan_validation" not in result
        if failure == "checkout":
            assert result["status"] == "blocked"
            assert "intake changed the read-only checkout" in result["findings"]
        else:
            assert all(result[key] == value for key, value in native.items())


def test_intake_catalog_explains_frozen_recipe_owners_and_report_support(gate_project):
    from devflow_temporal.delivery_feature_gates import intake_recipe_catalog
    from devflow_temporal.role_runner import _task

    spec, _plan, source = gate_project
    spec.update(provider="fake", feature_plan_version=2, work_id="work", goal="Bounded change")
    spec["feature_delivery"]["snapshot"] = {}
    spec["policy"]["roles"] = {"intake": {"model": "fake", "effort": "low"}}
    spec["policy"]["checks"] = [
        {"id": "lint", "argv": ["pnpm", "--filter", "@jobctrl/web", "lint"], "cwd": "."},
        {"id": "node-test", "argv": ["node", "--test", "test.test.mjs"], "cwd": "."},
    ]
    (source / "scripts").mkdir()
    (source / "scripts/checks.toml").write_text('''schema_version = 1
[checks.reported]
kind = "junit"
cwd = "."
argv = ["node", "scripts/reported.mjs", "{report_path}"]
''')
    git(source, "add", ".")
    git(source, "commit", "-qm", "Frozen report recipe")
    spec["base_sha"] = git(source, "rev-parse", "HEAD")
    catalog = intake_recipe_catalog(spec)
    by_id = {entry["recipe_id"]: entry for entry in catalog}
    assert by_id["lint"] == {
        "stage": "checks", "recipe_id": "lint", "selector_root": "apps/web",
        "named_selectors": False, "mandatory_selectors": [],
    }
    assert not by_id["node-test"]["named_selectors"]
    assert by_id["checks.reported"]["named_selectors"]
    assert by_id["job-list-browser"]["selector_root"] == "apps/web"
    assert by_id["job-list-browser"]["mandatory_selectors"] == [BULK, PAGINATION]
    task = _task({"spec": spec, "role": "intake", "iteration": 0, "workspace": str(source),
                  "candidate": {"id": "original", "head": spec["base_sha"]}})
    assert json.dumps(catalog, sort_keys=True) in task.goal
    assert "selectors=[] runs the whole unchanged recipe" in task.goal
    assert "apps/web/src/example.test.ts, not src/example.test.ts" in task.goal
    assert "Correct all controller plan_validation findings" in task.goal
