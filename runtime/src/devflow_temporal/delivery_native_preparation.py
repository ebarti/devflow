"""Frozen host identity and deterministic native command-boundary preparation."""

from __future__ import annotations

import hashlib
import json
import platform
import sys
from copy import deepcopy
from importlib.metadata import distribution, version
from pathlib import Path

from .contracts import digest
from .delivery_native_guard import NATIVE_OVERRIDES, protected_commands
from .delivery_native_process import NativeProcess
from .delivery_preparation import (
    PACKAGE,
    PreparationError,
    _hash,
    _lock,
    _private_bytes,
    _reference,
    _write,
    run_binding,
)
from .delivery_resources import RunResources, private_directory, read_private, write_private
from .delivery_sandbox import _native_env, _profile_lines
from .payload import payload_digest
from .runtime_dependencies import locked_dependency_identity

SCHEMA = "devflow-native-macos-environment-v1"


def native_identity(spec: dict) -> dict:
    if sys.platform != "darwin" or (
        spec["policy"].get("host_sandbox") != "trusted-local"
        and not Path("/usr/bin/sandbox-exec").is_file()
    ):
        raise PreparationError("native-macos execution requires macOS and its command sandbox")
    binary = Path(spec["policy"]["codex_bin"]).resolve(strict=True)
    bundled = Path(
        distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex")
    ).resolve(strict=True)
    dependencies = locked_dependency_identity()
    if spec["policy"]["runtime_dependencies"] != dependencies or binary != bundled:
        raise PreparationError("native CLI must be the same bundled executable as the locked SDK")
    actual = {
        name: version(name)
        for name in ("agent-runtime-kit", "openai-codex", "openai-codex-cli-bin")
    }
    expected = {
        "agent-runtime-kit": dependencies["kit_version"],
        "openai-codex": dependencies["codex_sdk_version"],
        "openai-codex-cli-bin": dependencies["codex_cli_version"],
    }
    if actual != expected:
        raise PreparationError("installed native dependencies differ from the frozen lock")
    if spec["policy"]["config_overrides"] != NATIVE_OVERRIDES:
        raise PreparationError("native roles must disable built-in agents and plugins")
    return {
        **({"execution_mode": "trusted-local"}
           if spec["policy"].get("host_sandbox") == "trusted-local" else {}),
        "platform": "native-macos",
        "os_version": platform.mac_ver()[0],
        "architecture": platform.machine(),
        "python": str(Path(sys.executable).resolve()),
        "python_sha256": _hash(Path(sys.executable).resolve()),
        "codex_bin": str(binary),
        "codex_bin_sha256": _hash(binary),
        "packages": actual,
        "runtime_dependencies": dependencies,
        "runtime_payload_sha256": payload_digest(PACKAGE),
        "config_overrides": NATIVE_OVERRIDES,
        "toolchain_roots": spec["policy"].get("toolchain_roots", []),
        "package_manager_cache": spec["policy"].get("package_manager_cache"),
        "browser_read_roots": (spec["policy"].get("browser_qa") or {}).get("read_roots", []),
        "protected_commands": [str(path) for path in protected_commands(str(binary))],
    }


def _measure(spec: dict, identity: dict, *, state_root: Path | None = None) -> dict:
    if identity.get("execution_mode") == "trusted-local":
        return _measure_trusted(spec, identity, state_root=state_root)
    root = RunResources(spec).scratch("preparation", "boundary")
    workspace, home, codex_home = root / "workspace", root / "home", root / "private-codex"
    scratch = home / "tmp"
    evidence = Path(spec["state_dir"]) / "native-preparation"
    for path in (workspace, home, codex_home, scratch, evidence):
        private_directory(path)
    protected = evidence / "controller-canary.json"
    write_private(protected, {"value": "SAFE"})
    copied = codex_home / "auth.json"
    write_private(copied, {"value": "SAFE"})
    lines = _profile_lines(
        "devflow-role",
        workspace=workspace,
        workspace_access="write",
        home=home,
        codex_home=codex_home,
        scratch=scratch,
        extra_read=(
            Path(sys.base_prefix),
            *(Path(root) for root in identity["toolchain_roots"]),
            *(
                (Path(identity["package_manager_cache"]),)
                if identity["package_manager_cache"]
                else ()
            ),
        ),
        protected_executables=protected_commands(identity["codex_bin"]),
    )
    config = codex_home / "config.toml"
    config.write_text("\n".join(lines) + "\n")
    config.chmod(0o600)
    probe = workspace / "probe.py"
    control_env = _native_env(home, codex_home, scratch)
    control_env["PATH"] = str(Path(identity["codex_bin"]).parent) + ":" + control_env["PATH"]
    path_control = NativeProcess(
        spec,
        evidence / "path-control",
        argv=["/bin/sh", "-c", "codex --version"],
        cwd=workspace,
        environment=control_env,
        timeout=10,
    ).run()
    expected_version = "codex-cli " + identity["runtime_dependencies"]["codex_cli_version"]
    if (
        path_control["exit_code"] != 0
        or Path(path_control["log"]).read_text().strip() != expected_version
    ):
        raise PreparationError("native shell positive control cannot find the locked Codex CLI")
    command_paths = [
        (identity["codex_bin"], "--version"),
        (str(PACKAGE.parents[1] / ".venv/bin/devflow-delivery"), "--help"),
    ]
    outside = Path("/private/tmp") / ("df-outside-" + digest(spec["run_id"])[:12])
    probe.write_text(
        "import json,os,pathlib,socket,subprocess,sys\n"
        f"protected=[pathlib.Path({str(protected)!r}),pathlib.Path({str(copied)!r})]\n"
        "result={}\n"
        "for number,path in enumerate(protected):\n"
        " try: path.read_bytes(); result[str(number)+'_read']='allowed'\n"
        " except PermissionError: result[str(number)+'_read']='denied'\n"
        " try: path.write_text('BREACH'); result[str(number)+'_write']='allowed'\n"
        " except PermissionError: result[str(number)+'_write']='denied'\n"
        "pathlib.Path('allowed.txt').write_text('OK')\n"
        "pathlib.Path(os.environ['TMPDIR'],'owned-tmp').write_text('OK')\n"
        "result['owned_tmp_write']=True\n"
        f"outside=pathlib.Path({str(outside)!r})\n"
        "try: outside.write_text('BREACH'); result['outside_tmp_write']='allowed'\n"
        "except PermissionError: result['outside_tmp_write']='denied'\n"
        "for name,address in [('provider_network',('1.1.1.1',443)),"
        "('controller_loopback',('127.0.0.1',9))]:\n"
        " sock=socket.socket(); sock.settimeout(2)\n"
        " try: sock.connect(address); result[name]='allowed'\n"
        " except PermissionError: result[name]='denied'\n"
        " except OSError as exc: result[name]='unproven:'+str(exc.errno)\n"
        " finally: sock.close()\n"
        f"for number,command in enumerate({command_paths!r}):\n"
        " try:\n"
        "  child=subprocess.run(command,capture_output=True,timeout=5)\n"
        "  result['metadata_'+str(number)]='allowed' if child.returncode==0 else 'denied'\n"
        " except PermissionError: result['metadata_'+str(number)]='denied'\n"
        f"binary={identity['codex_bin']!r}\n"
        "nested=[binary,'exec','--ignore-user-config','--skip-git-repo-check','--ephemeral',"
        "'--json','-m','gpt-6.1-sol','Reply OK']\n"
        "for name,command in [('nested_explicit',nested),"
        "('nested_shell',['/bin/sh','-c','exec codex '+"
        "' '.join(__import__('shlex').quote(value) for value in nested[1:])])]:\n"
        " child=subprocess.run(command,env={**os.environ,"
        "'PATH':str(pathlib.Path(binary).parent)+':'+os.environ['PATH']},"
        "capture_output=True,text=True,timeout=10)\n"
        " result[name]='denied' if child.returncode!=0 and "
        "'thread.started' not in child.stdout and 'response.' not in child.stdout "
        "and ('Operation not permitted' in child.stderr or 'Permission denied' in child.stderr "
        "or (name=='nested_shell' and child.returncode==127 "
        "and 'codex: not found' in child.stderr)) "
        "else 'failed_unproven'\n"
        " result[name+'_diagnostic']=child.stderr[-500:]\n"
        "if os.getenv('DEVFLOW_PROBE_CHILD')!='1':\n"
        " child=subprocess.run([sys.executable,__file__],"
        "env={**os.environ,'DEVFLOW_PROBE_CHILD':'1'},capture_output=True,text=True,check=True)\n"
        " result['child']=json.loads(child.stdout)\n"
        "print(json.dumps(result))\n"
    )
    process = NativeProcess(
        spec,
        evidence / "boundary",
        argv=[
            identity["codex_bin"],
            "sandbox",
            "-P",
            "devflow-role",
            "-C",
            str(workspace),
            "--",
            str(Path(sys.executable).resolve()),
            str(probe),
        ],
        cwd=workspace,
        environment=_native_env(home, codex_home, scratch),
        timeout=60,
    )
    outcome = process.run()
    if outcome["exit_code"] != 0 or outcome["cleanup"] != "observed-native-confirmed":
        raise PreparationError(
            "native command-boundary measurement failed; private native-preparation log is retained"
        )
    observed = json.loads(Path(outcome["log"]).read_text())
    _validate_observed(observed)
    if read_private(protected) != {"value": "SAFE"} or read_private(copied) != {"value": "SAFE"}:
        raise PreparationError("native command probe changed a protected canary")
    observed_path = evidence / "observed.json"
    write_private(observed_path, observed)
    return {
        "schema": SCHEMA,
        "identity": identity,
        "fingerprint": digest(identity),
        "measurement": {
            "path_control": _reference(Path(path_control["log"])),
            "observed": _reference(observed_path),
            "log": _reference(Path(outcome["log"])),
            "process_cleanup": outcome["cleanup"],
            "exit_code": outcome["exit_code"],
            "native_teardown_scope": outcome["native_teardown_scope"],
        },
    }


def _measure_trusted(spec: dict, identity: dict, *, state_root: Path | None = None) -> dict:
    """Measure the actual full-host launcher without asserting hostile-code isolation."""
    root = RunResources(spec).scratch("preparation", "trusted-local")
    workspace, home, codex_home = root / "workspace", root / "home", root / "codex"
    scratch = home / "tmp"
    evidence = Path(spec["state_dir"]) / "native-preparation" / "trusted-local"
    for path in (workspace, home, codex_home, scratch, evidence):
        private_directory(path)
    environment = _native_env(home, codex_home, scratch)
    control = NativeProcess(
        spec, evidence / "path-control", argv=[identity["codex_bin"], "--version"],
        cwd=workspace, environment=environment, timeout=10,
    ).run()
    probe = NativeProcess(
        spec, evidence / "trusted-local", cwd=workspace, environment=environment, timeout=30,
        argv=[sys.executable, "-I", "-c",
              "import json,os,pathlib; "
              "from devflow_temporal.delivery_native_guard import reject_nested_controller; "
              "pathlib.Path(os.environ['TMPDIR'],'probe').write_text('OK'); "
              "print(json.dumps({'mode':'trusted-local','host_read':pathlib.Path('/etc/hosts')"
              ".is_file(),'owned_tmp_write':True,"
              "'managed_depth':os.getenv('DEVFLOW_MANAGED_DEPTH')})); "
              "\ntry: reject_nested_controller()\nexcept ValueError: pass\n"
              "else: raise RuntimeError('nested controller accepted')"],
    ).run()
    if control["exit_code"] != 0 or probe["exit_code"] != 0:
        raise PreparationError("trusted-local launch measurement failed")
    observed = json.loads(Path(probe["log"]).read_text())
    _validate_observed(observed, trusted=True)
    observed_path = evidence / "observed.json"
    write_private(observed_path, observed)
    proof = {
        "schema": SCHEMA, "identity": identity, "fingerprint": digest(identity),
        "measurement": {
            "path_control": _reference(Path(control["log"])),
            "observed": _reference(observed_path), "log": _reference(Path(probe["log"])),
            "process_cleanup": probe["cleanup"], "exit_code": probe["exit_code"],
            "native_teardown_scope": probe["native_teardown_scope"],
        },
    }
    _validate(proof, identity, state_root or Path(spec["state_dir"]).parents[1])
    return proof


def _validate_observed(observed: dict, *, trusted: bool = False) -> None:
    if trusted:
        if observed != {"mode": "trusted-local", "host_read": True,
                        "owned_tmp_write": True, "managed_depth": "1"}:
            raise PreparationError("trusted-local launch or ancestry measurement did not pass")
        return
    for item in (observed, observed.get("child", {})):
        if any(
            item.get(key) != "denied"
            for key in (
                "0_read",
                "0_write",
                "1_read",
                "1_write",
                "nested_explicit",
                "nested_shell",
                "provider_network",
                "controller_loopback",
                "outside_tmp_write",
            )
        ):
            raise PreparationError("native filesystem or recursive-command boundary did not pass")
        if item.get("owned_tmp_write") is not True:
            raise PreparationError("native command boundary cannot write its owned temporary root")


def _validate(proof: dict, identity: dict, state_root: Path) -> None:
    if (
        proof.get("schema") != SCHEMA
        or proof.get("identity") != identity
        or proof.get("fingerprint") != digest(identity)
    ):
        raise PreparationError("native preparation proof is stale")
    measurement = proof["measurement"]
    if (
        measurement.get("process_cleanup") != "observed-native-confirmed"
        or measurement.get("exit_code") != 0
    ):
        raise PreparationError("native measurement did not establish observed teardown")
    for key in ("observed", "log", "path_control"):
        reference = measurement[key]
        path = Path(reference["path"])
        content = _private_bytes(path, state_root / "runs")
        if hashlib.sha256(content).hexdigest() != reference["sha256"]:
            raise PreparationError("native measured evidence changed")
        if key == "observed":
            _validate_observed(json.loads(content),
                               trusted=identity.get("execution_mode") == "trusted-local")
        if key == "path_control" and content.decode().strip() != (
            "codex-cli " + identity["runtime_dependencies"]["codex_cli_version"]
        ):
            raise PreparationError("native recursive-command probe lacks a valid PATH control")


def verify_native_spec(spec: dict) -> None:
    prepared = spec.get("preparation", {})
    if not isinstance(prepared, dict) or prepared.get("schema") != SCHEMA:
        raise PreparationError("native execution authority has not been frozen or changed")
    identity = native_identity(spec)
    if (
        spec["policy"].get("native_identity") != identity
        or prepared.get("schema") != SCHEMA
        or prepared.get("fingerprint") != digest(identity)
    ):
        raise PreparationError("native execution authority has not been frozen or changed")
    root = Path(spec["state_dir"]).parents[1]
    reference = prepared["environment"]
    path = Path(reference["path"])
    if path != root / "preparation-native" / digest(identity) / "proof.json":
        raise PreparationError("native proof left its canonical private cache")
    content = _private_bytes(path, root / "preparation-native")
    if (
        hashlib.sha256(content).hexdigest() != reference["sha256"]
        or reference["sha256"] != spec["policy"].get("environment_proof_sha256")
        or run_binding(spec) != spec["policy"].get("security_binding_sha256")
        or prepared["security_binding_sha256"] != run_binding(spec)
        or digest(spec["policy"]) != spec["policy_digest"]
    ):
        raise PreparationError("native prepared run binding changed")
    _validate(json.loads(content), identity, root)


def bind_native_spec(spec: dict, identity: dict, path: Path, *, reused: bool) -> dict:
    effective = deepcopy(spec)
    effective["policy"].update(
        native_identity=identity,
        codex_bin_sha256=identity["codex_bin_sha256"],
        environment_proof_sha256=_hash(path),
    )
    effective["policy"]["security_binding_sha256"] = run_binding(effective)
    effective["policy_digest"] = digest(effective["policy"])
    effective["preparation"] = {
        "schema": SCHEMA,
        "fingerprint": digest(identity),
        "environment": _reference(path),
        "security_binding_sha256": run_binding(effective),
        "cache_reused": reused,
    }
    verify_native_spec(effective)
    return effective


def prepare_native_authority(store, spec: dict) -> dict:
    root = Path(spec["state_dir"]).parents[1]
    with _lock(root / "preparation-native"):
        existing = store.prepared_spec(spec["run_id"])
        if existing:
            verify_native_spec(existing)
            return existing
        store.preparation_progress(
            spec["run_id"],
            "native",
            "Verifying the locked native macOS runtime and command boundary",
        )
        identity = native_identity(spec)
        path = root / "preparation-native" / digest(identity) / "proof.json"
        reused = path.exists() or path.is_symlink()
        proof = (
            json.loads(_private_bytes(path, root / "preparation-native"))
            if reused
            else _measure(spec, identity)
        )
        _validate(proof, identity, root)
        if not reused:
            _write(path, proof)
        effective = bind_native_spec(spec, identity, path, reused=reused)
        return store.freeze_preparation(spec, effective)
