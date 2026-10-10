"""Exercise future-file gate admission and actual per-source test reports."""

from __future__ import annotations

import json
import subprocess
import sys
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
    spec, plan, source = gate_project
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


@pytest.mark.parametrize("bad", ["dropped_final", "unadmitted", "duplicate", "outside_authority"])
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


@pytest.mark.parametrize("bad", ["untracked", "symlink", "duplicate"])
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


def test_real_pytest_junit_proves_each_selected_source_and_rejects_other_file(tmp_path):
    source = tmp_path / "source"
    tests = source / "tests"
    tests.mkdir(parents=True)
    git(source, "init", "-q")
    for name in ("test_present.py", "test_other.py"):
        (tests / name).write_text("def test_actual(): assert True\n")
    git(source, "add", ".")
    recipe = {
        "required_selectors": ["tests/test_present.py", "tests/test_other.py"],
        "selector_evidence_version": 1,
        "selector_project": ".",
    }
    sources = resolve_required_selectors(recipe, source)
    report = tmp_path / "actual-junit.xml"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/test_present.py",
            "-q",
            "--junitxml=" + str(report),
        ],
        cwd=source,
        check=True,
        capture_output=True,
    )
    with pytest.raises(ValueError, match="test_other.py"):
        selector_evidence(recipe, source, sources, junit=report.read_bytes())
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/test_present.py",
            "tests/test_other.py",
            "-q",
            "--junitxml=" + str(report),
        ],
        cwd=source,
        check=True,
        capture_output=True,
    )
    receipts = selector_evidence(recipe, source, sources, junit=report.read_bytes())
    assert all(item["passed"] == 1 for item in receipts)


@pytest.mark.parametrize("failure", [None, "ignored", "outside_authority", "candidate_excluded"])
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
