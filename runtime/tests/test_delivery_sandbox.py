from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from devflow_temporal.delivery_sandbox import prepare_sandbox


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
