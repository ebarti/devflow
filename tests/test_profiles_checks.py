import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from devflow.checks import run_check
from devflow.errors import WorkflowError
from devflow.profiles import assert_admitted_profile, load_profile


def profile(tmp_path, command, kind="junit"):
    directory = tmp_path / "repo"
    (directory / ".devflow").mkdir(parents=True)
    (directory / ".devflow/repository.toml").write_text(
        'schema_version = 1\n[repository]\nid = "fixture"\ndefault_branch = "main"\n'
    )
    (directory / ".devflow/workflow.lock").write_text(
        'schema_version = 1\nversion = "0.1.0"\nrevision = "' + "a" * 40 + '"\n'
    )
    (directory / ".devflow/checks.toml").write_text(
        "schema_version = 1\n[checks.fixture]\nkind = "
        + json.dumps(kind)
        + '\ndescription = "Fixture invariants"\nargv = '
        + json.dumps(command)
        + "\n"
    )
    return load_profile(directory)


def candidate():
    return {
        "candidate_id": "C-1",
        "tree_sha": "a" * 40,
        "dependency_hash": "b" * 64,
        "environment_hash": "c" * 64,
    }


def run(tmp_path, loaded, **kwargs):
    artifacts = []

    def save(data):
        artifacts.append(json.loads(data))
        return hashlib.sha256(data).hexdigest()

    result = run_check(
        loaded,
        "fixture",
        candidate(),
        acceptance_ids=["AC-1"],
        state_dir=tmp_path / "private",
        put_artifact=save,
        **kwargs,
    )
    return result, artifacts


def test_real_command_and_junit_proof(tmp_path):
    script = (
        "from pathlib import Path; import sys; "
        "Path(sys.argv[1]).write_text('<testsuite><testcase name=\"works\"/></testsuite>')"
    )
    loaded = profile(tmp_path, [sys.executable, "-c", script, "{report_path}"])
    result, artifacts = run(tmp_path, loaded)
    assert result["execution_status"] == "PASS"
    assert result["executed_assertions"] == 1
    assert "junit" in artifacts[0]
    assert not list((tmp_path / "private/checks").iterdir())


@pytest.mark.parametrize(
    "xml,status",
    [
        ("<testsuite/>", "FAIL"),
        ("<testsuite><testcase><skipped/></testcase></testsuite>", "FAIL"),
        ("<testsuite><testcase><failure/></testcase></testsuite>", "FAIL"),
        ("garbage", "BLOCKED"),
    ],
)
def test_zero_exit_does_not_make_product_proof(tmp_path, xml, status):
    loaded = profile(tmp_path, ["fixture", "{report_path}"])

    def execute(argv, **kwargs):
        Path(argv[-1]).write_text(xml)
        return subprocess.CompletedProcess(argv, 0, "", "")

    result, _ = run(tmp_path, loaded, runner=execute)
    assert result["execution_status"] == status


def test_missing_report_and_launch_failure_are_not_pass(tmp_path):
    loaded = profile(tmp_path, ["/definitely/missing/devflow-check", "{report_path}"])
    result, _ = run(tmp_path, loaded)
    assert result["execution_status"] == "BLOCKED"
    assert result["process_status"] == "not_started"


def test_forged_report_link_cannot_prove_check_or_delete_foreign_file(tmp_path):
    loaded = profile(tmp_path, ["fixture", "{report_path}"])
    sentinel = tmp_path / "unrelated.xml"
    sentinel.write_text('<testsuite><testcase name="not-executed"/></testsuite>')

    def execute(argv, **kwargs):
        Path(argv[-1]).symlink_to(sentinel)
        return subprocess.CompletedProcess(argv, 0, "", "")

    result, _ = run(tmp_path, loaded, runner=execute)
    assert result["execution_status"] == "BLOCKED"
    assert "not-executed" in sentinel.read_text()


def test_profile_edit_cannot_lower_its_own_admitted_check(tmp_path):
    loaded = profile(tmp_path, ["git", "diff", "--check"], kind="static")
    admitted = "sha256:" + loaded.fingerprint
    assert_admitted_profile(loaded, admitted)
    checks = loaded.root / ".devflow/checks.toml"
    checks.write_text(checks.read_text().replace('"git", "diff", "--check"', '"true"'))
    with pytest.raises(WorkflowError, match="changed after admission"):
        assert_admitted_profile(load_profile(loaded.root), admitted)


def test_float_pin_and_escape_are_rejected(tmp_path):
    loaded = profile(tmp_path, ["true"], kind="static")
    lock = loaded.root / ".devflow/workflow.lock"
    lock.write_text(lock.read_text().replace("a" * 40, "main"))
    with pytest.raises(WorkflowError, match="full Git"):
        load_profile(loaded.root)
