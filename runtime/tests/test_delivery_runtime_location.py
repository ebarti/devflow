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


@pytest.fixture(scope="module")
def installed_pair(tmp_path_factory):
    assert not command(["git", "-C", ROOT, "status", "--porcelain"]).strip(), (
        "Commit the isolated candidate before testing actual installed-source authority"
    )
    root = tmp_path_factory.mktemp("location")
    python = Path(
        os.environ.get(
            "DEVFLOW_TEST_RUNTIME_LOCATION_PYTHON",
            str(
                Path.home()
                / ".local/share/uv/python/cpython-3.12.13-macos-aarch64-none/bin/python3.12"
            ),
        )
    )
    assert python.is_file(), "provide the original frozen native interpreter for this owner test"
    home = root / "home"
    old = home / ".codex/worktrees/owned/developer-workflow"
    new = home / ".local/share/devflow/developer-workflow"
    environment = {
        **os.environ,
        "HOME": str(home),
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "UV_CACHE_DIR": str(Path.home() / ".cache/uv"),
        "UV_PYTHON_INSTALL_DIR": str(Path(sys.executable).resolve().parents[2]),
        "PYTHONDONTWRITEBYTECODE": "1",
        "DEVFLOW_TEST_TEMPORAL_PREREQUISITES": os.environ.get(
            "DEVFLOW_TEST_TEMPORAL_PREREQUISITES",
            str(ROOT.parent / "temporal-prerequisites-20261024"),
        ),
        "DEVFLOW_TEST_TEMPORAL_SDK_PYTHON": os.environ.get(
            "DEVFLOW_TEST_TEMPORAL_SDK_PYTHON",
            str(ROOT.parent / "devflow-temporal-sdk-test-fixture/environment/bin/python"),
        ),
    }
    for target, revision in ((old, BASE), (new, "HEAD")):
        target.parent.mkdir(parents=True, exist_ok=True)
        command(["git", "clone", "--quiet", "--no-hardlinks", "--dissociate", ROOT, target])
        command(["git", "-C", target, "checkout", "--quiet", "--detach", revision])
        command(
            ["uv", "sync", "--frozen", "--offline", "--python", python],
            cwd=target / "runtime",
            env=environment,
        )
    return root, old, new, environment, python


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
