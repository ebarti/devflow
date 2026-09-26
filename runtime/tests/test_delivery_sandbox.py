from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from devflow_temporal.delivery_sandbox import _profile_lines, prepare_sandbox


def test_legacy_launcher_rejects_real_provider_before_credential_copy(tmp_path):
    with pytest.raises(ValueError, match="only for the fake provider"):
        prepare_sandbox({"spec": {"provider": "codex"}}, tmp_path / "attempt")


@pytest.fixture
def slash_tmp_path():
    with tempfile.TemporaryDirectory(prefix="devflow-native-profile-", dir="/tmp") as path:
        yield Path(path)


@pytest.mark.skipif(not os.environ.get("DEVFLOW_CODEX_BIN"), reason="real Codex CLI path required")
def test_native_profile_restricts_sibling_tmp_paths_and_alias(slash_tmp_path: Path):
    """A real Codex sandbox must not inherit broad /tmp access from its cwd."""

    binary = os.environ["DEVFLOW_CODEX_BIN"]
    root = slash_tmp_path
    assert root.resolve().is_relative_to(Path("/private/tmp"))
    workspace = root / "workspace"
    workspace.mkdir()
    home = root / "role-home"
    codex_home = home / "codex"
    scratch = home / "tmp"
    for path in (home, codex_home, scratch):
        path.mkdir(mode=0o700, exist_ok=True)
    controller = root / "controller.txt"
    outside = root / "outside.txt"
    controller.write_text("SAFE")
    outside.write_text("SAFE")
    alias = Path("/private/tmp") / root.resolve().relative_to("/private/tmp") / "outside.txt"
    profile = _profile_lines(
        "devflow-role",
        workspace=workspace,
        workspace_access="write",
        home=home,
        codex_home=codex_home,
        scratch=scratch,
    )
    (codex_home / "config.toml").write_text("\n".join(profile) + "\n")
    probe = workspace / "probe.py"
    probe.write_text(
        "import json, os, pathlib, subprocess, sys\n"
        f"paths={{'controller':pathlib.Path({str(controller)!r}),"
        f"'outside':pathlib.Path({str(outside)!r}),"
        f"'alias':pathlib.Path({str(alias)!r})}}\n"
        "observed={}\n"
        "for key,path in paths.items():\n"
        " try: path.read_text(); observed[key+'_read']='allowed'\n"
        " except PermissionError: observed[key+'_read']='denied'\n"
        " try: path.write_text('BREACH'); observed[key+'_write']='allowed'\n"
        " except PermissionError: observed[key+'_write']='denied'\n"
        "(pathlib.Path(__file__).parent/'allowed.txt').write_text('OK')\n"
        "if os.getenv('PROBE_CHILD')!='1':\n"
        " child=subprocess.run([sys.executable,__file__],"
        "env={**os.environ,'PROBE_CHILD':'1'},capture_output=True,text=True,check=True)\n"
        " observed['child']=json.loads(child.stdout)\n"
        "print(json.dumps(observed))\n"
    )
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(home),
        "CODEX_HOME": str(codex_home),
        "TMPDIR": str(scratch),
        "USER": os.environ.get("USER", ""),
        "LANG": "C",
    }
    result = subprocess.run(
        [
            binary,
            "sandbox",
            "-P",
            "devflow-role",
            "-C",
            str(workspace),
            "/usr/bin/python3",
            str(probe),
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    observed = json.loads(result.stdout)
    for item in (observed, observed["child"]):
        for key in ("controller", "outside", "alias"):
            assert item[f"{key}_read"] == "denied"
            assert item[f"{key}_write"] == "denied"
    assert controller.read_text() == outside.read_text() == "SAFE"
    assert (workspace / "allowed.txt").read_text() == "OK"


@pytest.mark.skipif(not os.environ.get("DEVFLOW_CODEX_BIN"), reason="real Codex CLI path required")
def test_independent_profile_reads_only_bound_diff_not_git_or_controller(slash_tmp_path: Path):
    binary = os.environ["DEVFLOW_CODEX_BIN"]
    root = slash_tmp_path
    workspace = root / "review-checkout"
    workspace.mkdir()
    (workspace / ".git").write_text("gitdir: private\n")
    state = root / "controller"
    evidence = state / "gate-evidence" / "0" / "review"
    evidence.mkdir(parents=True)
    patch = evidence / "candidate.patch"
    patch.write_text("BOUND_DIFF\n")
    protected = state / "private.txt"
    protected.write_text("SAFE\n")
    home = state / "runs" / "run" / "role-homes" / "review" / "0"
    codex_home = home / "codex"
    scratch = home / "tmp"
    for path in (home, codex_home, scratch):
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
    profile = _profile_lines(
        "devflow-role",
        workspace=workspace,
        workspace_access="read",
        home=home,
        codex_home=codex_home,
        scratch=scratch,
        extra_read=(patch,),
    )
    (codex_home / "config.toml").write_text("\n".join(profile) + "\n")
    probe = (
        "import json,pathlib,sys\n"
        "paths=[pathlib.Path(value) for value in sys.argv[1:]]\n"
        "observed={}\n"
        "for name,path in zip(('diff','controller','git'),paths):\n"
        " try: observed[name]=path.read_text()\n"
        " except PermissionError: observed[name]='denied'\n"
        "try: paths[0].write_text('BREACH'); observed['diff_write']='allowed'\n"
        "except PermissionError: observed['diff_write']='denied'\n"
        "print(json.dumps(observed))\n"
    )
    result = subprocess.run(
        [
            binary,
            "sandbox",
            "-P",
            "devflow-role",
            "-C",
            str(workspace),
            "/usr/bin/python3",
            "-c",
            probe,
            str(patch),
            str(protected),
            str(workspace / ".git"),
        ],
        env={
            "PATH": "/usr/bin:/bin",
            "HOME": str(home),
            "CODEX_HOME": str(codex_home),
            "TMPDIR": str(scratch),
            "USER": os.environ.get("USER", ""),
            "LANG": "C",
        },
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert json.loads(result.stdout) == {
        "diff": "BOUND_DIFF\n",
        "controller": "denied",
        "git": "denied",
        "diff_write": "denied",
    }
    assert patch.read_text() == "BOUND_DIFF\n"
    assert protected.read_text() == "SAFE\n"


@pytest.mark.skipif(not Path("/usr/bin/sandbox-exec").is_file(), reason="macOS Seatbelt required")
def test_outer_role_boundary_blocks_state_and_child_writes(tmp_path: Path, monkeypatch):
    root = tmp_path / "state"
    state_dir = root / "runs" / "run-1"
    attempt = state_dir / "attempts" / "job-1"
    workspace = root / "checkouts" / "run-1"
    workspace.mkdir(parents=True)
    state_dir.mkdir(parents=True)
    attempt.mkdir(parents=True)
    attempt.chmod(0o700)
    secret = root / "broker-secret"
    secret.write_text("SAFE")
    external = tmp_path / "external-secret"
    external.write_text("SAFE")
    config = tmp_path / "config.json"
    config.write_text("{}")
    database = tmp_path / "workflow.sqlite3"
    database.write_text("SAFE")
    spec = {
        "provider": "fake",
        "state_dir": str(state_dir),
        "config_path": str(config),
        "policy": {"tracking_db": str(database)},
    }
    monkeypatch.setenv("GH_TOKEN", "owned-fixture-secret")
    original_codex_auth = Path.home() / ".codex" / "auth.json"
    profile, env = prepare_sandbox(
        {"spec": spec, "role": "implement", "workspace": str(workspace)}, attempt
    )
    assert "GH_TOKEN" not in env
    child_code = (
        "from pathlib import Path; Path(" + repr(str(external)) + ").write_text('CHILD-BREACH')"
    )
    program = f"""
import json, os, subprocess
from pathlib import Path
secret=Path({str(secret)!r})
external=Path({str(external)!r})
workspace=Path({str(workspace)!r})
credential=Path({str(original_codex_auth)!r})
results={{}}
for label,path in [('broker',secret),('external',external)]:
    try:
        path.write_text('BREACH')
        results[label+'_write']='allowed'
    except PermissionError:
        results[label+'_write']='denied'
try:
    secret.read_text()
    results['broker_read']='allowed'
except PermissionError:
    results['broker_read']='denied'
try:
    credential.read_bytes()
    results['credential_read']='allowed'
except PermissionError:
    results['credential_read']='denied'
results['inherited_gh_token']='GH_TOKEN' in os.environ
gh=subprocess.run(['gh','auth','status'],capture_output=True,text=True)
results['gh_authenticated']=gh.returncode==0
child=subprocess.run([{sys.executable!r},'-c',{child_code!r}],capture_output=True,text=True)
results['child_write_exit']=child.returncode
(workspace/'allowed.txt').write_text('OK')
print(json.dumps(results))
"""
    result = subprocess.run(
        ["/usr/bin/sandbox-exec", "-f", str(profile), sys.executable, "-c", program],
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    observed = json.loads(result.stdout)
    assert observed == {
        "broker_write": "denied",
        "external_write": "denied",
        "broker_read": "denied",
        "credential_read": "denied",
        "inherited_gh_token": False,
        "gh_authenticated": False,
        "child_write_exit": 1,
    }
    assert secret.read_text() == external.read_text() == "SAFE"
    assert (workspace / "allowed.txt").read_text() == "OK"
