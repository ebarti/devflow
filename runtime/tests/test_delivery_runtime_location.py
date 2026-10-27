"""Independent installed environments, absent old code and public location ownership.

The host-plugin CLI is an owned protocol double. Native measurement, imported
SDK identity, Git/RECORD inspection, private evidence, Store/resource/admission
readers and process teardown use their actual owners in isolated subprocesses.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
BASE = "1ebfc71ea5522ee74dbf6cd820f01649e3197bca"
SCENARIO = Path(__file__).with_name("runtime_location_scenario.py")
pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS installation")


def command(argv, *, cwd=None, env=None, timeout=180):
    result = subprocess.run(
        list(map(str, argv)), cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout
    )
    assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-7000:]
    return result.stdout


def installed_pair_fixture(tmp_path_factory, *, historical_venv=False):
    assert not command(["git", "-C", ROOT, "status", "--porcelain"]).strip(), (
        "Commit the isolated candidate before testing actual installed-source authority"
    )
    root = tmp_path_factory.mktemp("location")
    python = Path(os.environ.get("DEVFLOW_TEST_RUNTIME_LOCATION_PYTHON", sys._base_executable))
    assert python.is_file(), "provide a native Python 3.12 interpreter for this owner test"
    from portable_temporal_fixture import fixture_root

    stage = fixture_root()
    home = root / "home"
    old = home / ".codex/worktrees/owned/developer-workflow"
    new = home / ".local/share/devflow/developer-workflow"
    environment = {
        **os.environ,
        "HOME": str(home),
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "UV_CACHE_DIR": os.environ.get("UV_CACHE_DIR", str(Path.home() / ".cache/uv")),
        "UV_PYTHON_INSTALL_DIR": str(Path(sys.executable).resolve().parents[2]),
        "PYTHONDONTWRITEBYTECODE": "1",
        "DEVFLOW_PORTABLE_FIXTURE_ROOT": str(stage),
    }
    for target, revision in ((old, BASE), (new, "HEAD")):
        target.parent.mkdir(parents=True, exist_ok=True)
        command(["git", "clone", "--quiet", "--no-hardlinks", "--dissociate", ROOT, target])
        command(["git", "-C", target, "checkout", "--quiet", "--detach", revision])
        sync_fixture_runtime(target, python, environment, historical_venv=historical_venv)
    return root, old, new, environment, python


def sync_fixture_runtime(target, python, environment, *, historical_venv=False):
    runtime = target / "runtime"
    if historical_venv:
        # Historical validators accepted only the editable controller's .pth.
        # Precreate a standard Python environment so uv 0.12.17 installs the
        # same locked dependencies without adding its own virtualenv hook.
        command([python, "-m", "venv", "--without-pip", runtime / ".venv"], env=environment)
    command(["uv", "sync", "--frozen", "--offline", "--python", python],
            cwd=runtime, env=environment)
    if historical_venv:
        registrations = list((runtime / ".venv/lib").glob("python*/site-packages/*.pth"))
        assert len(registrations) == 1
        assert registrations[0].name == "_editable_impl_devflow_temporal.pth"
        assert registrations[0].read_text().strip() == str(runtime / "src")


@pytest.fixture(scope="module")
def installed_pair(tmp_path_factory):
    return installed_pair_fixture(tmp_path_factory)


def test_historical_fixture_sync_retains_only_its_editable_registration(tmp_path):
    runtime = tmp_path / "candidate" / "runtime"
    package = runtime / "src/devflow_temporal"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VALUE = 'fixture'\n")
    (runtime / "pyproject.toml").write_text(
        '[build-system]\nrequires = ["hatchling"]\nbuild-backend = "hatchling.build"\n'
        '[project]\nname = "devflow-temporal"\nversion = "0.1.0"\n'
        'requires-python = ">=3.12"\n'
        '[tool.hatch.build.targets.wheel]\npackages = ["src/devflow_temporal"]\n'
    )
    env = {**os.environ, "UV_EXCLUDE_NEWER": "false"}
    python = Path(sys._base_executable)
    command(["uv", "lock", "--offline"], cwd=runtime, env=env)
    sync_fixture_runtime(runtime.parent, python, env, historical_venv=True)
    assert command([runtime / ".venv/bin/python", "-I", "-c",
                    "import devflow_temporal; print(devflow_temporal.VALUE)"],
                   env=env).strip() == "fixture"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", ["scope", "browser", "stale1169", "stale1173", "rollback", "pendingrollback"]
)
async def test_public_permanent_location_owner_and_continuation(installed_pair, kind):
    from temporal_test_server import local_temporal

    root, old_template, new, environment, python = installed_pair
    # Each retained run has a distinct original managed location and owner. The
    # fixed permanent runtime stays the same; no live or host directory is used.
    old = root / "home/.codex/worktrees" / kind / "developer-workflow"
    command(["git", "clone", "--quiet", "--no-hardlinks", "--dissociate", old_template, old])
    command(
        ["uv", "sync", "--frozen", "--offline", "--python", python],
        cwd=old / "runtime",
        env=environment,
    )
    case = root / kind
    async with local_temporal() as temporal:
        bound = {
            **environment,
            "DEVFLOW_TEST_LOCATION_TEMPORAL_ADDRESS": (
                temporal.client.service_client.config.target_host
            ),
        }
        command([old / "runtime/.venv/bin/python", "-I", SCENARIO, "seed", case, kind], env=bound)
        shutil.rmtree(old)
        assert not old.exists()
        output = command(
            [new / "runtime/.venv/bin/python", "-I", SCENARIO, "exercise", case, kind],
            env=bound,
            timeout=420,
        )
        if kind == "scope":
            for line in output.splitlines():
                if ("actual_native_post_ack_public_get" in line
                        or "actual_native_post_admission_public_reads" in line):
                    print(line)
    result = json.loads(output.splitlines()[-1])
    if kind in {"rollback", "pendingrollback"}:
        assert result["state"] == "rolled_back"
        assert result["legacy_runtime_ready"] is False
        assert result["ack_row_unchanged"] is True
        assert result["original_proof_preserved"] is True
        assert result["original_package_bytes"] is True
        assert result["legacy_guard_retained"] is True
        if kind == "pendingrollback":
            assert result["pending_only"] is True and result["location_permission"] is False
        else:
            assert result["historical_proof_readable"] is True
        return
    assert result["old_runtime_absent"] is True
    assert result["ack_row_unchanged"] is True
    assert result["cohort_size"] == 6
    assert result["strict_bundle"] is True
    assert result["finite_maximum"] == 4
    assert result["native_preparation"] == "actual"
    assert result["capacity"] == 2 and result["frozen_capacity"] == 6


def test_worker_failure_diagnostic_retains_verdict_without_untrusted_text(tmp_path):
    from runtime_location_scenario import assert_native_checks_passed, worker_failure_diagnostic

    sentinel = "Bearer synthetic-secret-do-not-log"
    result = {"outcome": "blocked", "error": sentinel, "findings": [sentinel],
              "roles": [{"role": sentinel, "status": sentinel, "findings": [sentinel]}]}
    checks = [{"state": "failed", "source_unchanged": True, "diagnostic": sentinel,
               "results": [{"id": "worker-regressions", "passed": False, "exit_code": 1,
                            "test_count": 2, "cleanup": "confirmed",
                            "evidence_failure": sentinel, "rejection_causes": [sentinel],
                            "diagnostic": sentinel},
                           {"id": sentinel, "passed": sentinel, "exit_code": sentinel}]}]
    diagnostic = worker_failure_diagnostic(result, ["checks", sentinel], checks)
    assert sentinel not in json.dumps(diagnostic)
    assert diagnostic["error_category"] == "other" and diagnostic["finding_count"] == 1
    assert diagnostic["calls"] == ["checks", "other"]
    assert diagnostic["checks"][0]["state"] == "failed"
    item, unknown = diagnostic["checks"][0]["results"]
    assert item == {"id": "worker-regressions", "passed": False, "exit_code": 1,
                    "test_count": 2, "cleanup": "confirmed", "evidence_failure_present": True,
                    "rejection_count": 1, "diagnostic_present": True}
    assert unknown["id"] == "other" and unknown["exit_code"] is None

    import xml.etree.ElementTree as ET

    from devflow_temporal.delivery_check_evidence import retain_artifacts

    candidate = {"id": "a" * 64}
    folder = tmp_path / "checks/4/worker-regressions"
    raw = folder / "pytest-artifacts"
    raw.mkdir(parents=True)
    suite = ET.Element("testsuite")
    case = ET.SubElement(suite, "testcase", name="test_real_local_activity")
    ET.SubElement(case, "failure", type="RuntimeError", message=(
        "Failed starting Temporal dev server: timed out " + sentinel
    )).text = sentinel
    case = ET.SubElement(suite, "testcase", name=sentinel)
    ET.SubElement(case, "error", type=sentinel, message=sentinel)
    (raw / "junit.xml").write_bytes(ET.tostring(suite))
    reference = retain_artifacts(folder, candidate)
    owned = {"state": "failed", "results": [
        {"id": "worker-regressions", "passed": False, "artifacts": reference},
    ]}
    diagnostic = worker_failure_diagnostic(
        {**result, "candidate": candidate}, ["checks"], [owned], state_dir=tmp_path,
    )
    assert sentinel not in json.dumps(diagnostic)
    inner = diagnostic["checks"][0]["results"][0]["inner_junit"]
    assert inner["state"] == "available" and inner["unknown_failed_case_count"] == 1
    assert inner["failed_cases"] == [{"probe": "test_real_local_activity", "outcome": "failure",
                                      "exception_type": "RuntimeError",
                                      "known_startup_timeout": True}]
    (folder / "artifacts/junit.xml").write_text(sentinel)
    diagnostic = worker_failure_diagnostic(
        {**result, "candidate": candidate}, ["checks"], [owned], state_dir=tmp_path,
    )
    assert sentinel not in json.dumps(diagnostic) and diagnostic["outcome"] == "blocked"
    assert diagnostic["checks"][0]["results"][0]["inner_junit"] == {
        "state": "unavailable", "exception_type": "ValueError",
    }

    def refused_cleanup(receipt):
        with pytest.raises(AssertionError) as failure:
            assert_native_checks_passed(result, ["checks"], [{"state": "failed", "results": [
                {"id": "worker-regressions", "passed": False, "artifact_cleanup": receipt},
            ]}])
        assert sentinel not in str(failure.value)
        reported = json.loads(str(failure.value))
        assert reported["outcome"] == "blocked"
        assert reported["checks"][0]["results"][0]["passed"] is False
        return reported["checks"][0]["results"][0]["artifact_cleanup"]

    def receipt(reason):
        return {"state": "failed_unknown", "reason": reason, "path": sentinel,
                "provider_output": sentinel, "log": sentinel}

    assert refused_cleanup(receipt(
        "disposable check artifact root has a live holder (count=2, pids=123,456)"
    )) == {"state": "failed_unknown", "refusal_category": "live_holder",
           "holder_count": 2, "holder_pids": [123, 456]}
    assert refused_cleanup(receipt(
        "disposable check artifact root holder inspection failed (returncode=-9)"
    )) == {"state": "failed_unknown", "refusal_category": "inspection_failed",
           "inspection_returncode": -9}
    for reason, category in [
        ("disposable check artifact root was replaced", "root_replaced"),
        ("disposable check output lost an original native journal", "journal_changed"),
        ("registered native processes or ports remain active", "native_actor_or_port_live"),
    ]:
        assert refused_cleanup(receipt(reason)) == {
            "state": "failed_unknown", "refusal_category": category,
        }
    for reason in [
        sentinel, {"reason": sentinel},
        "disposable check artifact root has a live holder (count=2, pids=123)",
        "disposable check artifact root has a live holder (count=1, pids=2147483648)",
        "disposable check artifact root has a live holder (count=1000001, pids=123)",
        "disposable check artifact root has a live holder (count=1, pids=123) " + sentinel,
        "disposable check artifact root holder inspection failed (returncode=1)",
        "disposable check artifact root holder inspection failed (returncode=256)",
    ]:
        assert refused_cleanup(receipt(reason)) == {
            "state": "failed_unknown", "refusal_category": "other",
        }
    for malformed in [None, [sentinel], {"state": [sentinel], "reason": sentinel}]:
        assert refused_cleanup(malformed) == {"state": "other", "refusal_category": "other"}

def test_native_check_assertion_omits_untrusted_failure_output():
    from runtime_location_scenario import assert_native_checks_passed

    sentinel = "Bearer synthetic-secret-do-not-log"
    with pytest.raises(AssertionError) as failure:
        assert_native_checks_passed(
            {"outcome": "delivered", "error": sentinel, "findings": [sentinel]},
            ["checks"],
            [{"state": "failed", "diagnostic": sentinel, "results": [
                {"id": "worker-regressions", "passed": False, "diagnostic": sentinel}
            ]}],
        )
    assert sentinel not in str(failure.value)
    assert json.loads(str(failure.value))["checks"][0]["results"][0]["passed"] is False

def test_native_check_assertion_reports_first_failed_check_after_retry():
    from runtime_location_scenario import assert_native_checks_passed

    checks = [{"state": "failed", "results": [{"id": "worker-regressions", "passed": False}]},
              {"state": "passed", "results": [{"id": "worker-regressions", "passed": True}]}]
    with pytest.raises(AssertionError) as failure:
        assert_native_checks_passed({"outcome": "delivered"}, ["checks", "checks"], checks)
    verdicts = json.loads(str(failure.value))["checks"]
    assert [item["state"] for item in verdicts] == ["failed", "passed"]
    assert verdicts[0]["results"][0]["passed"] is False
