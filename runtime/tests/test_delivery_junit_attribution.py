"""Frozen wrapper package provenance and actionable controller evidence failures."""

from __future__ import annotations

import json
import subprocess
import xml.etree.ElementTree as ET
from copy import deepcopy
from types import SimpleNamespace

import pytest
from test_delivery_store import service as service

from devflow_temporal.candidate import candidate_for
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_feature_gates import resolve_required_selectors, selector_evidence
from devflow_temporal.delivery_role_evidence import allocate, read_context
from devflow_temporal.delivery_workflow import _broker_findings

SELECTORS = ["apps/api/test/server.test.ts", "apps/api/benchmark/local-scale.test.ts"]


@pytest.fixture(autouse=True)
def local_git_signing(monkeypatch):
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "commit.gpgsign")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "false")


def git(source, *args):
    return subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "-C", str(source), *args],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def junit(cases):
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite")
    for origin, outcome in cases:
        case = ET.SubElement(suite, "testcase", classname=origin, name="actual case")
        if outcome != "passed":
            ET.SubElement(case, outcome)
    return ET.tostring(root)


@pytest.fixture
def wrapped_project(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    git(source, "init", "-q")
    git(source, "config", "user.name", "Fixture")
    git(source, "config", "user.email", "fixture@example.invalid")
    for relative in [*SELECTORS, "apps/api/test/other.test.ts"]:
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("test('owned source',()=>{});\n")
    (source / "apps/api/package.json").write_text('{"name":"@fixture/api"}')
    git(source, "add", ".")
    git(source, "commit", "-qm", "Frozen package owner")
    recipe = {
        "id": "wrapped-api", "cwd": ".", "kind": "test", "junit_required": True,
        "argv": ["node", "--input-type=module", "-e",
                 "import {spawnSync} from 'node:child_process';"
                 "const child=spawnSync('corepack',['pnpm','--filter','@fixture/api',"
                 "'exec','vitest','run','--reporter=junit'],{stdio:'inherit'});"
                 "process.exit(child.status??1);"],
        "required_selectors": SELECTORS.copy(),
        "selector_project": ".", "selector_evidence_version": 1,
    }
    spec = {"provider": "fake", "source_path": str(source),
            "base_sha": git(source, "rev-parse", "HEAD"),
            "policy": {"host_sandbox": "trusted-local", "checks": [deepcopy(recipe)]}}
    return source, recipe, spec


def test_package_relative_junit_uses_only_frozen_command_owner(wrapped_project):
    source, recipe, spec = wrapped_project
    sources = resolve_required_selectors(recipe, source)
    before = deepcopy((recipe, spec, sources))
    report = junit([("test/server.test.ts", "passed"),
                    ("benchmark/local-scale.test.ts", "passed")])
    with pytest.raises(ValueError, match="no complete passing evidence"):
        selector_evidence(recipe, source, sources, junit=report)
    # Mutable candidate package metadata cannot replace its immutable base owner.
    (source / "apps/api/package.json").write_text('{"name":"@other/package"}')
    alternative = source / "apps/other"
    alternative.mkdir()
    (alternative / "package.json").write_text('{"name":"@fixture/api"}')
    receipts = selector_evidence(recipe, source, sources, junit=report, spec=spec)
    assert {r["selector"]: r["passed"] for r in receipts} == dict.fromkeys(SELECTORS, 1)
    assert all(not r["failed"] and not r["skipped"] for r in receipts)
    assert (recipe, spec, sources) == before


@pytest.mark.parametrize("failure", [
    "missing", "failed", "skipped", "basename", "dynamic", "multiple", "unadmitted",
    "duplicate-owner", "source-drift", "ambiguous-path",
])
def test_wrapper_cannot_qualify_missing_ambiguous_or_changed_sources(wrapped_project, failure):
    source, recipe, spec = wrapped_project
    cases = [("test/server.test.ts", "passed"), ("benchmark/local-scale.test.ts", "passed")]
    if failure == "missing":
        cases[0] = ("test/other.test.ts", "passed")
    elif failure in {"failed", "skipped"}:
        cases[0] = (cases[0][0], "failure" if failure == "failed" else "skipped")
    elif failure == "basename":
        cases[0] = ("server.test.ts", "passed")
    elif failure == "dynamic":
        recipe["argv"][3] = recipe["argv"][3].replace("'@fixture/api'", "packageName")
        spec["policy"]["checks"] = [deepcopy(recipe)]
    elif failure == "multiple":
        recipe["argv"][3] += "spawnSync('corepack',['pnpm','--filter','@fixture/api','exec',"
        recipe["argv"][3] += "'vitest','run']);"
        spec["policy"]["checks"] = [deepcopy(recipe)]
    elif failure == "unadmitted":
        recipe["argv"][3] = recipe["argv"][3].replace("'@fixture/api'", "'@other/package'")
    elif failure == "duplicate-owner":
        duplicate = source / "apps/other/package.json"
        duplicate.parent.mkdir()
        duplicate.write_text('{"name":"@fixture/api"}')
        git(source, "add", ".")
        git(source, "commit", "-qm", "Ambiguous frozen package")
        spec["base_sha"] = git(source, "rev-parse", "HEAD")
    elif failure == "ambiguous-path":
        relative = "apps/api/apps/api/test/server.test.ts"
        path = source / relative
        path.parent.mkdir(parents=True)
        path.write_text("test('ambiguous',()=>{});\n")
        git(source, "add", relative)
        recipe["required_selectors"].append(relative)
        cases[0] = (SELECTORS[0], "passed")
    sources = resolve_required_selectors(recipe, source)
    if failure == "source-drift":
        (source / SELECTORS[0]).write_text("test('changed',()=>{});\n")
    with pytest.raises(ValueError):
        selector_evidence(recipe, source, sources, junit=junit(cases), spec=spec)


@pytest.mark.parametrize("missing", [False, True])
def test_broker_keeps_package_evidence_and_failure_on_zero_exit(
    wrapped_project, tmp_path, monkeypatch, missing,
):
    source, recipe, spec = wrapped_project
    state = tmp_path / "state"
    evidence = state / "checks/5"
    candidate = candidate_for(source)
    actual_run = subprocess.run

    def executor(argv, **kwargs):
        if argv == recipe["argv"]:
            folder = evidence / recipe["id"] / "pytest-artifacts"
            folder.mkdir(parents=True)
            folder.joinpath("junit.xml").write_bytes(junit([
                ("test/other.test.ts" if missing else "test/server.test.ts", "passed"),
                ("benchmark/local-scale.test.ts", "passed"),
            ]))
            return subprocess.CompletedProcess(argv, 0, "2 passed\n", "")
        return actual_run(argv, **kwargs)

    # Only the isolated fixture command is replaced; Git, source hashing, JUnit
    # retention and broker result construction use the real production path.
    monkeypatch.setattr(subprocess, "run", executor)
    broker = SimpleNamespace(spec=spec, state_dir=state)
    result = DeliveryBroker._run_check_list(broker, source, [recipe], evidence, candidate)
    row = result["results"][0]
    assert result["state"] == ("failed" if missing else "passed")
    assert result["source_unchanged"] and row["exit_code"] == 0 and row["test_count"] == 2
    assert row["junit"]["failures"] == row["junit"]["errors"] == row["junit"]["skipped"] == 0
    if missing:
        assert "apps/api/test/server.test.ts" in row["evidence_failure"]
    else:
        assert {r["selector"] for r in row["selectors"]} == set(SELECTORS)


def test_evidence_failure_survives_bounded_feedback_and_private_receipts(service, monkeypatch):
    from test_delivery_role_evidence import setup

    _store, broker, request = setup(service)
    failure = "required selector contributed no complete passing evidence: " + SELECTORS[0]
    failure += "x" * 1500
    result = {"state": "failed", "candidate_id": request["candidate"]["id"],
              "results": [{"id": "wrapped-api", "passed": False, "exit_code": 0,
                           "test_count": 1139, "diagnostic": "1139 passed",
                           "evidence_failure": failure,
                           "native_process": {"private controller": "must remain hidden"}}]}
    before = deepcopy(result)
    findings = _broker_findings("local_checks", result, iteration=5)
    feedback = json.loads(findings[0].split(": ", 1)[1])
    assert feedback["failed_checks"][0]["evidence_failure"] == failure[:1000]
    request["evidence_context"] = {"checks": {"local": result}}
    retained = read_context(allocate(request, "a" * 64))
    assert retained["checks"]["local"]["results"][0]["evidence_failure"] == failure
    assert "native_process" not in retained["checks"]["local"]["results"][0]
    assert result == before
    # Past workflow commands keep their historical feedback and job inputs.
    import devflow_temporal.delivery_workflow as protocol

    monkeypatch.setattr(protocol.workflow, "in_workflow", lambda: True)
    monkeypatch.setattr(protocol.workflow, "patched", lambda key: False)
    legacy = json.loads(_broker_findings("local_checks", result, iteration=5)[0].split(": ", 1)[1])
    assert "evidence_failure" not in legacy["failed_checks"][0]


def test_frozen_wrapper_report_survives_fixture_cleanup_and_verdict_replay(
    wrapped_project, tmp_path,
):
    import os
    import shlex
    import shutil
    import sys
    from pathlib import Path

    from test_delivery_resources import spec as owned_spec

    from devflow_temporal.delivery_check_evidence import junit_counts, retain_artifacts
    from devflow_temporal.delivery_feature_gates import retained_junit
    from devflow_temporal.delivery_native_process import NativeProcess
    from devflow_temporal.delivery_resources import RunResources

    source, recipe, authority = wrapped_project
    # The frozen wrapper derives its report directory from the controller's
    # basetemp, including the dedicated disposable fixtures child.
    recipe['argv'][3] = (
        "import path from 'node:path';import fs from 'node:fs';"
        "import {spawnSync} from 'node:child_process';"
        "const value=process.env.PYTEST_ADDOPTS||'';"
        "if(!value.startsWith('--basetemp='))throw Error('controller artifact path missing');"
        "const root=value.slice(11).replace(/^'|'$/g,'');fs.mkdirSync(root,{recursive:true});"
        "const child=spawnSync('corepack',['pnpm','--filter','@fixture/api','exec','vitest',"
        "'run','--reporter=default','--reporter=junit','--outputFile.junit='+"
        "path.join(root,'junit.xml')],{stdio:'inherit',env:{...process.env,"
        "PYTHONPATH:path.resolve('workers/automation/src')}});"
        "if(child.error)throw child.error;process.exit(child.status??1);"
    )
    authority['policy']['checks'] = [deepcopy(recipe)]
    candidate = candidate_for(source)
    sources = resolve_required_selectors(recipe, source)
    report = junit([('test/server.test.ts', 'passed'), ('benchmark/local-scale.test.ts', 'passed')])
    runner = tmp_path / 'bin/corepack'
    runner.parent.mkdir()
    runner.write_text(
        f'#!{sys.executable}\n'
        'import sys; from pathlib import Path\n'
        "assert sys.argv[1:7] == ['pnpm','--filter','@fixture/api','exec','vitest','run']\n"
        "assert sys.argv[7:9] == ['--reporter=default','--reporter=junit']\n"
        "assert sys.argv[9].startswith('--outputFile.junit=')\n"
        f'Path(sys.argv[9].split("=",1)[1]).write_bytes({report!r})\n'
    )
    runner.chmod(0o700)
    owned = owned_spec(tmp_path)
    resources = RunResources(owned)
    raw = resources.check_artifacts(
        Path(owned['state_dir']) / 'checks/7/wrapped-api/pytest-artifacts')
    inode = raw.stat().st_ino
    environment = {'PATH': str(runner.parent) + os.pathsep + os.environ['PATH'],
                   'PYTEST_ADDOPTS': '--basetemp=' + shlex.quote(str(raw / 'fixtures'))}
    node = str(Path(shutil.which('node')).resolve())
    process = NativeProcess(owned, raw.parent / 'native', argv=[node, *recipe['argv'][1:]],
                            cwd=source, environment=environment, timeout=5)
    result = process.run()
    assert result['exit_code'] == 0 and raw.stat().st_ino == inode
    reference = retain_artifacts(raw.parent, candidate)
    manifest = json.loads(Path(reference['path']).read_text())
    assert [item['relative_path'] for item in manifest['artifacts']] == ['fixtures/junit.xml']
    assert resources.cleanup_check_artifacts(raw, [process.journal])['state'] == 'removed'
    assert not raw.exists()
    for _ in range(2):
        assert process.run() == result
        assert retain_artifacts(raw.parent, candidate) == reference
        assert retained_junit(reference, candidate['id'], Path(owned['state_dir'])) == report
        assert junit_counts(reference, candidate['id'], Path(owned['state_dir'])) == {
            'tests': 2, 'failures': 0, 'errors': 0, 'skipped': 0, 'passed': 2,
        }
        receipts = selector_evidence(recipe, source, sources, junit=retained_junit(
            reference, candidate['id'], Path(owned['state_dir'])), spec=authority)
        assert {r['selector']: r['passed'] for r in receipts} == dict.fromkeys(SELECTORS, 1)
        assert not raw.exists()
    assert candidate_for(source) == candidate
    assert resources.finalize('blocked')['resource_cleanup'] == 'confirmed'
