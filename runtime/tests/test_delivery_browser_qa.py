from __future__ import annotations

import json
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path

import pytest

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_sandbox import prepare_browser_qa


def _git(path: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    ).stdout.strip()


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class _EffectStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        with self._connect() as db:
            db.execute(
                """CREATE TABLE delivery_effects (
                effect_key TEXT PRIMARY KEY,run_id TEXT,kind TEXT,request_json TEXT,
                state TEXT,observed_json TEXT,updated_at TEXT)"""
            )

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path)
        try:
            yield db
            db.commit()
        finally:
            db.close()


@pytest.mark.skipif(not Path("/usr/bin/sandbox-exec").is_file(), reason="macOS Seatbelt")
def test_broker_browser_qa_owns_ports_and_binds_receipt_to_candidate(tmp_path: Path):
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Fixture")
    _git(source, "config", "user.email", "fixture@example.invalid")
    (source / "README.md").write_text("Owned fixture\n")
    _git(source, "add", "README.md")
    _git(source, "commit", "-qm", "fixture")
    ports = (_free_port(), _free_port())
    assert ports[0] != ports[1]
    program = (
        "import os,socket,time; "
        "ports=[int(os.environ[x]) for x in ('QA_API_PORT','QA_WEB_PORT')]; "
        "servers=[socket.socket() for _ in ports]; "
        "[(s.bind(('127.0.0.1',p)),s.listen()) for s,p in zip(servers,ports)]; "
        "[(lambda c,p:(c.connect(('127.0.0.1',p)),c.close()))(socket.socket(),p) "
        "for p in ports]; "
        "time.sleep(2); print('2 passed',flush=True)"
    )
    qa = {
        "id": "browser-qa",
        "argv": ["/usr/bin/python3", "-c", program],
        "ports": {"QA_API_PORT": ports[0], "QA_WEB_PORT": ports[1]},
        "env": {"JOBCTRL_E2E_ISOLATED": "1"},
        "read_roots": [],
        "test_count_regex": r"(?m)(\d+) passed",
        "min_tests": 2,
        "timeout_seconds": 30,
        "artifact_paths": [],
    }
    state_root = tmp_path / "state"
    state_dir = state_root / "runs" / "fixture"
    state_dir.mkdir(parents=True)
    spec = {
        "run_id": "fixture",
        "provider": "codex",
        "source_path": str(source),
        "checkout": str(source),
        "state_dir": str(state_dir),
        "base_sha": _git(source, "rev-parse", "HEAD"),
        "policy_digest": "fixture-policy",
        "policy": {
            "host_sandbox": "native-profile",
            "toolchain_roots": [],
            "package_manager_cache": None,
            "browser_qa": qa,
        },
    }
    broker = DeliveryBroker(_EffectStore(tmp_path / "effects.sqlite3"), spec)
    candidate = broker.candidate()
    result = broker.run_browser_qa(0, candidate)
    try:
        assert result["state"] == "passed"
        assert result["test_count"] == 2
        assert result["exit_code"] == 0
        assert result["cleanup"] == "confirmed"
        assert result["source_unchanged"] is True
        assert all(result["listeners"][str(port)] for port in ports)
        assert json.loads(Path(result["receipt"]).read_text())["candidate_id"] == candidate["id"]
        assert broker.run_browser_qa(0, candidate) == result
        with socket.socket() as conflict:
            conflict.bind(("127.0.0.1", ports[0]))
            with pytest.raises(RuntimeError, match="unavailable"):
                broker.run_browser_qa(1, candidate)
    finally:
        shutil.rmtree(result["scratch"])


@pytest.mark.skipif(not Path("/usr/bin/sandbox-exec").is_file(), reason="macOS Seatbelt")
def test_browser_qa_profile_denies_credentials_state_outside_and_unrelated_network(
    tmp_path: Path,
):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    state_dir = tmp_path / "state" / "runs" / "fixture"
    evidence = state_dir / "browser-qa" / "0"
    evidence.mkdir(parents=True)
    protected = state_dir / "protected.txt"
    protected.write_text("SAFE")
    outside = tmp_path / "outside.txt"
    outside.write_text("SAFE")
    ports = (_free_port(), _free_port())
    qa = {
        "ports": {"QA_API_PORT": ports[0], "QA_WEB_PORT": ports[1]},
        "env": {"JOBCTRL_E2E_ISOLATED": "1"},
        "read_roots": [],
    }
    spec = {
        "provider": "codex",
        "state_dir": str(state_dir),
        "policy": {
            "host_sandbox": "native-profile",
            "toolchain_roots": [],
            "package_manager_cache": None,
        },
    }
    scratch = Path(tempfile.mkdtemp(prefix="dfqa-test-", dir="/private/tmp"))
    try:
        profile, env = prepare_browser_qa(spec, checkout, evidence, scratch, qa)
        program = r"""
import json,os,socket,subprocess,sys
from pathlib import Path
def result(fn):
 try: fn(); return 'ALLOWED'
 except Exception as exc: return type(exc).__name__+':'+str(getattr(exc,'errno',None))
def connect(host,port):
 sock=socket.socket(); sock.settimeout(1)
 try: sock.connect((host,port))
 finally: sock.close()
def bind(port):
 sock=socket.socket()
 try: sock.bind(('127.0.0.1',port))
 finally: sock.close()
state,outside,outside_tmp,other=sys.argv[1:5]
link=Path(os.environ['TMPDIR'])/('escape-'+str(os.getpid()))
link.symlink_to(outside)
observed={
 'state_read':result(lambda:Path(state).read_text()),
 'outside_read':result(lambda:Path(outside).read_text()),
 'symlink_escape_read':result(lambda:link.read_text()),
 'outside_write':result(lambda:Path(outside).write_text('BREACH')),
 'slash_tmp_read':result(lambda:Path('/tmp',Path(outside_tmp).name).read_text()),
 'unrelated_port_connect':result(lambda:connect('127.0.0.1',int(other))),
 'unrelated_port_bind':result(lambda:bind(int(other))),
 'unrelated_host_connect':result(lambda:connect('1.1.1.1',443)),
 'allowed_write':result(lambda:Path('allowed.txt').write_text('OK')),
}
if os.environ.get('QA_CHILD')!='1':
 child=subprocess.run(['/usr/bin/python3','-c',os.environ['PROBE_CODE'],state,outside,outside_tmp,other],
  env={**os.environ,'QA_CHILD':'1'},capture_output=True,text=True,check=True)
 observed['child']=json.loads(child.stdout)
print(json.dumps(observed))
"""
        env["PROBE_CODE"] = program
        descriptor, name = tempfile.mkstemp(prefix="dfqa-unowned-", dir="/private/tmp")
        outside_tmp = Path(name)
        with os.fdopen(descriptor, "w") as stream:
            stream.write("SAFE")
        other = _free_port()
        while other in ports:
            other = _free_port()
        observed = subprocess.run(
            [
                "/usr/bin/sandbox-exec",
                "-f",
                str(profile),
                "/usr/bin/python3",
                "-c",
                program,
                str(protected),
                str(outside),
                str(outside_tmp),
                str(other),
            ],
            cwd=checkout,
            env=env,
            capture_output=True,
            text=True,
            check=True,
        )
        result = json.loads(observed.stdout)
        for item in (result, result["child"]):
            for field in (
                "state_read",
                "outside_read",
                "symlink_escape_read",
                "outside_write",
                "slash_tmp_read",
                "unrelated_port_connect",
                "unrelated_port_bind",
                "unrelated_host_connect",
            ):
                assert item[field] == "PermissionError:1"
            assert item["allowed_write"] == "ALLOWED"
        assert outside.read_text() == outside_tmp.read_text() == protected.read_text() == "SAFE"
    finally:
        shutil.rmtree(scratch)
        if "outside_tmp" in locals():
            outside_tmp.unlink()
