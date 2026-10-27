"""Versioned requirements and bindings for execution-environment readiness.

A binding is an observation, never a launch grant or a product-gate result.
The caller must also verify current resource custody immediately before effects.
"""
from __future__ import annotations

import hashlib
import os
import re
import stat
import subprocess
from pathlib import Path

from .candidate import candidate_for
from .contracts import digest

_SCHEMA = 1
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_LOCK_NAMES = frozenset({"pyproject.toml", "uv.lock", "package.json", "pnpm-lock.yaml",
                         "pnpm-workspace.yaml", "package-lock.json", "yarn.lock"})
_MAX_METADATA = 4 * 1024 * 1024


class ReadinessError(ValueError):
    """A readiness observation is missing, unsupported, or stale."""


def validate_declaration(value, policy):
    """Validate operator-frozen requirements without accepting commands or paths."""
    if (not isinstance(value, dict)
            or not {"version", "generation", "browser_engines", "temporal_checks"} <= set(value)
            or set(value) - {"version", "generation", "browser_engines", "temporal_checks",
                             "temporal_provenance"}
            or value.get("version") != _SCHEMA or type(value.get("version")) is not int):
        raise ReadinessError("readiness declaration has an unsupported shape or version")
    generation = value["generation"]
    if type(generation) is not int or not 0 <= generation <= 2**31 - 1:
        raise ReadinessError("readiness generation must be a bounded nonnegative integer")
    engines, checks = value["browser_engines"], value["temporal_checks"]
    if (not isinstance(engines, list) or len(engines) > 3
            or any(not isinstance(item, str) or item not in {"chromium", "firefox", "webkit"}
                   for item in engines)
            or engines != sorted(set(engines))):
        raise ReadinessError("browser engines must be a unique sorted supported list")
    if bool(engines) != bool(policy.get("browser_qa")):
        raise ReadinessError("browser QA requires an explicit nonempty engine declaration")
    if (not isinstance(checks, list) or len(checks) > 32
            or any(not isinstance(item, str) or not _ID.fullmatch(item) for item in checks)
            or checks != sorted(set(checks))):
        raise ReadinessError("Temporal check IDs must be bounded, unique and sorted")
    recipes = [c for stage in ("baseline_checks", "prepublish_checks", "checks")
               for c in policy.get(stage, []) if isinstance(c, dict)]
    for check_id in checks:
        matches = [c for c in recipes if c.get("id") == check_id]
        # A check may appear in baseline and prepublication as one exact recipe.
        unique = {digest(item): item for item in matches}
        if (len(unique) != 1 or next(iter(unique.values())).get("kind") != "test"
                or not any(item.get("id") == check_id for item in policy.get("checks", [])
                           if isinstance(item, dict))):
            raise ReadinessError("Temporal requirement must name one frozen local test recipe")
    provenance = value.get("temporal_provenance")
    if checks:
        if (len(checks) != 1 or not isinstance(provenance, dict)
                or set(provenance) != {"check_id", "python_project", "provenance_path",
                                       "provenance_sha256"}
                or provenance.get("check_id") != checks[0]
                or not isinstance(provenance.get("python_project"), str)
                or not isinstance(provenance.get("provenance_path"), str)
                or not Path(provenance["provenance_path"]).is_absolute()
                or not isinstance(provenance.get("provenance_sha256"), str)
                or not re.fullmatch(r"[0-9a-f]{64}", provenance["provenance_sha256"])):
            raise ReadinessError("Temporal readiness requires one bounded provenance declaration")
    elif provenance is not None:
        raise ReadinessError("Temporal provenance requires its declared check")
    return {"version": _SCHEMA, "generation": generation,
            "browser_engines": list(engines), "temporal_checks": list(checks),
            **({"temporal_provenance": dict(provenance)} if provenance is not None else {})}


def derive_declaration(policy):
    """Derive only requirements that frozen recipes prove have no hidden backend."""
    if policy.get("browser_qa"):
        raise ReadinessError(
            "repository readiness declaration required: declare browser QA engines")
    recipes = [item for stage in ("baseline_checks", "prepublish_checks", "checks")
               for item in policy.get(stage, []) if isinstance(item, dict)]
    if any(item.get("kind") == "test" or item.get("test_count_regex") for item in recipes):
        raise ReadinessError(
            "repository readiness declaration required: declare test backend requirements")
    for item in recipes:
        argv = item.get("argv")
        if (not isinstance(argv, list) or not argv or not isinstance(argv[0], str)
                or not (Path(argv[0]).name == "ruff" and len(argv) > 1
                        and argv[1] in {"check", "format"}
                        or argv == ["git", "diff", "--check"])):
            raise ReadinessError(
                "repository readiness declaration required: unknown check requirements")
    return validate_declaration({"version": 1, "generation": 0,
                                 "browser_engines": [], "temporal_checks": []}, policy)


def describe_requirements(spec, checkout, accepted_plan=None):
    """Describe only explicitly frozen recipes; prose never supplies commands."""
    policy = spec.get("policy") or {}
    declaration = validate_declaration(policy.get("readiness"), policy)
    checkout = Path(checkout)
    if not checkout.is_absolute() or checkout.resolve(strict=True) != checkout:
        raise ReadinessError("readiness checkout is not its exact canonical directory")
    plan = spec.get("accepted_plan", "")
    if accepted_plan is not None and accepted_plan != plan:
        raise ReadinessError("readiness plan differs from accepted authority")
    from .delivery_configured_resources import implementation_prerequisites
    configured = implementation_prerequisites(spec)
    recipes = [{"stage": stage, "check": check}
               for stage, key in (("prechecks", "prepublish_checks"), ("checks", "checks"))
               for check in policy.get(key, [])]
    if len(recipes) > 128 or any(not isinstance(item["check"], dict) for item in recipes):
        raise ReadinessError("readiness check inventory is malformed or unbounded")
    return {"version": _SCHEMA, "declaration": declaration,
            "configured": configured, "check_recipes": recipes,
            "browser_qa": policy.get("browser_qa"),
            "accepted_plan_sha256": digest(plan) if accepted_plan is not None else None,
            "policy_sha256": digest(policy)}


def _metadata(checkout):
    try:
        result = subprocess.run(["git", "-c", "core.fsmonitor=false", "-C", str(checkout),
                                 "ls-files", "-z"], check=True, capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:
        raise ReadinessError("cannot enumerate tracked readiness metadata") from exc
    paths = result.stdout.split(b"\0")
    if len(paths) > 100000 or len(result.stdout) > 16 * 1024 * 1024:
        raise ReadinessError("tracked readiness metadata inventory is unbounded")
    observed = {}
    for raw in paths:
        if not raw:
            continue
        relative = os.fsdecode(raw)
        if Path(relative).name not in _LOCK_NAMES:
            continue
        path = checkout / relative
        if path.is_symlink() or path.resolve(strict=True) != path:
            raise ReadinessError("readiness metadata is not fixed regular source")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size > _MAX_METADATA:
                raise ReadinessError("readiness metadata is not bounded regular source")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                content = stream.read(_MAX_METADATA + 1)
            after = os.fstat(fd)
            named = path.lstat()
            if ((before.st_dev, before.st_ino) != (named.st_dev, named.st_ino)
                    or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns,
                 before.st_ctime_ns) !=
                (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns,
                 after.st_ctime_ns) or len(content) != before.st_size):
                raise ReadinessError("readiness metadata changed while measured")
            observed[relative] = hashlib.sha256(content).hexdigest()
        finally:
            os.close(fd)
    return observed



def _candidate_matches(checkout, candidate):
    current = candidate_for(checkout)
    return isinstance(candidate, dict) and all(candidate.get(key) == value
                                               for key, value in current.items())

def binding(spec, candidate, requirements, *, resource_generation):
    """Measure current source and immutable execution identity, not a grant."""
    checkout = Path(spec["checkout"])
    if checkout.resolve(strict=True) != checkout or not _candidate_matches(checkout, candidate):
        raise ReadinessError("readiness candidate changed")
    if (spec.get("provider") == "codex" and spec.get("readiness_version") == 1
            and (candidate.get("base_sha") != spec.get("base_sha")
                 or candidate.get("policy_digest") != spec.get("policy_digest"))):
        raise ReadinessError("readiness candidate authority changed")
    if type(resource_generation) is not int or resource_generation < 0:
        raise ReadinessError("readiness resource generation is unavailable")
    if requirements.get("policy_sha256") != digest(spec.get("policy")):
        raise ReadinessError("readiness policy differs from frozen execution")
    return {"version": _SCHEMA, "spec_sha256": digest(spec), "candidate": candidate,
            "requirements_sha256": digest(requirements), "metadata": _metadata(checkout),
            "resource_generation": resource_generation}


def _verify_observations(store, spec, candidate, requirements, receipt):
    """Authenticate a complete observed pass without granting execution."""
    from .delivery_preparation import verify_prepared_spec
    from .delivery_readiness_verify import browser, preparation

    if store.effective_spec(spec["run_id"]) != spec:
        raise ReadinessError("readiness execution authority changed")
    stage, iteration, attempt = (receipt.get("stage"), receipt.get("iteration"),
                                 receipt.get("attempt"))
    if (stage not in {"configured", "plan"} or type(iteration) is not int or iteration < 0
            or type(attempt) is not int or attempt < 0):
        raise ReadinessError("readiness receipt stage is malformed")
    selected = Path(receipt.get("workspace", ""))
    _validate_workspace(spec, selected, candidate, iteration)
    expected = describe_requirements(
        spec, selected, accepted_plan=spec.get("accepted_plan", "") if stage == "plan" else None,
    )
    if requirements != expected:
        raise ReadinessError("readiness requirements changed")
    resources = resource_binding(spec, workspace=selected)
    transient = resources[str(Path(spec["state_dir"]) / "transient")]
    subject = binding(spec, candidate, requirements, resource_generation=transient["generation"])
    subject["resources"] = resources
    if receipt.get("binding") != subject:
        raise ReadinessError("readiness receipt no longer matches candidate or resources")
    verify_prepared_spec(spec)
    needed = {stage, "tools"}
    if requirements["browser_qa"]:
        needed.add("browser")
    if requirements["declaration"]["temporal_checks"]:
        needed.add("temporal")
    observations = receipt.get("observations")
    if not isinstance(observations, dict) or set(observations) != needed:
        raise ReadinessError("readiness receipt omitted a declared observation")
    preparation(store, spec, candidate, requirements, receipt)
    from .delivery_readiness_verify import tools as verify_tools

    verify_tools(store, spec, candidate, requirements, receipt)
    if "browser" in needed:
        browser(store, spec, candidate, requirements, receipt)
    if "temporal" in needed:
        from .delivery_readiness_verify import temporal

        temporal(store, spec, candidate, requirements, receipt)
    if (resource_binding(spec, workspace=selected) != resources
            or store.effective_spec(spec["run_id"]) != spec
            or not _candidate_matches(Path(spec["checkout"]), candidate)):
        raise ReadinessError("readiness owner changed during evidence authentication")


def verify_receipt(store, spec, candidate, receipt, requirements=None):
    """Authenticate a saved receipt; final execution still needs a fresh launch fence."""
    from .delivery_resources import read_private

    if not isinstance(receipt, dict) or receipt.get("version") != _SCHEMA:
        raise ReadinessError("readiness receipt is malformed")
    stage, attempt = receipt.get("stage"), receipt.get("attempt")
    if (stage not in {"configured", "plan"} or type(attempt) is not int or attempt < 0
            or receipt.get("state") != "passed"):
        raise ReadinessError("readiness receipt has no authenticated pass")
    path = (Path(spec["state_dir"]) / "readiness" / "receipts" / stage
            / str(receipt.get("iteration")) / f"{attempt}.json")
    if receipt.get("receipt") != str(path):
        raise ReadinessError("readiness receipt left its owned path")
    try:
        raw = read_private(path, raw=True)
    except (OSError, ValueError) as exc:
        raise ReadinessError("readiness receipt is unavailable") from exc
    if (len(raw) > 8 * 1024 * 1024
            or hashlib.sha256(raw).hexdigest() != receipt.get("receipt_sha256")):
        raise ReadinessError("readiness receipt changed")
    import json

    if json.loads(raw) != {k: v for k, v in receipt.items()
                           if k not in {"receipt", "receipt_sha256"}}:
        raise ReadinessError("readiness receipt differs from its immutable evidence")
    if requirements is None:
        requirements = describe_requirements(
            spec, Path(receipt["workspace"]),
            accepted_plan=spec.get("accepted_plan", "") if stage == "plan" else None,
        )
    _verify_observations(store, spec, candidate, requirements, receipt)


# A fixed controller script; the candidate cannot provide probe commands or flags.
# The package must resolve through the checkout, as with the admitted QA command.
_BROWSER_PROBE = r"""
const fs = require('node:fs');
const path = require('node:path');
const root = fs.realpathSync(process.argv[1]);
const engines = JSON.parse(process.argv[2]);
let entry;
try { entry = require.resolve('@playwright/test', {paths: [process.cwd()]}); }
catch (_) { entry = require.resolve('playwright', {paths: [process.cwd()]}); }
entry = fs.realpathSync(entry);
const relative = path.relative(root, entry);
if (relative.startsWith('..' + path.sep) || relative === '..' || path.isAbsolute(relative))
  throw new Error('Playwright resolved outside candidate checkout');
const playwright = require(entry);
(async () => {
  const executables = {};
  for (const engine of engines) {
    const browserType = playwright[engine];
    if (!browserType || typeof browserType.launch !== 'function' ||
        typeof browserType.executablePath !== 'function')
      throw new Error('candidate Playwright lacks declared browser engine');
    let selected = browserType.executablePath();
    if (engine === 'chromium') {
      const packageEntry = require.resolve('playwright', {paths: [path.dirname(entry)]});
      const coreEntry = require.resolve('playwright-core', {paths: [path.dirname(packageEntry)]});
      let bundlePath;
      try {
        bundlePath = require.resolve('playwright-core/lib/coreBundle',
                                     {paths: [path.dirname(packageEntry)]});
      } catch (error) {
        if (error.code !== 'MODULE_NOT_FOUND' && error.code !== 'ERR_PACKAGE_PATH_NOT_EXPORTED')
          throw error;
      }
      const registry = bundlePath ? require(bundlePath).registry?.registry :
        require(path.join(path.dirname(coreEntry), 'lib/server/registry/index.js')).registry;
      const shell = registry && registry.findExecutable('chromium-headless-shell');
      if (!shell || typeof shell.executablePath !== 'function')
        throw new Error('candidate Playwright lacks its pinned Chromium headless shell');
      selected = shell.executablePath();
    }
    const executable = fs.realpathSync(selected);
    const browser = await browserType.launch({headless: true, executablePath: executable});
    await browser.close();
    executables[engine] = executable;
  }
  const modules = Object.keys(require.cache).map(name => fs.realpathSync(name)).sort();
  console.log('DEVFLOW_READINESS_BROWSER=' + JSON.stringify({entry, executables, modules}));
})().catch(error => { console.error(String(error)); process.exitCode = 1; });
"""


def _file_reference(raw, *, maximum=256 * 1024 * 1024):
    path = Path(raw)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ReadinessError("readiness executable path is not canonical")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= maximum:
            raise ReadinessError("readiness executable is not bounded regular data")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            value = hashlib.file_digest(stream, "sha256").hexdigest()
        after = os.fstat(fd)
        fields = ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")
        named = path.lstat()
        if (any(getattr(before, key) != getattr(after, key) for key in fields)
                or (named.st_dev, named.st_ino) != (before.st_dev, before.st_ino)):
            raise ReadinessError("readiness executable changed while measured")
        return {"path": str(path), "sha256": value, "size": before.st_size}
    finally:
        os.close(fd)




_BROWSER_RESOLVE = r"""
const fs = require('node:fs');
const path = require('node:path');
const root = fs.realpathSync(process.argv[1]);
let entry;
try { entry = require.resolve('@playwright/test', {paths: [process.cwd()]}); }
catch (_) { entry = require.resolve('playwright', {paths: [process.cwd()]}); }
entry = fs.realpathSync(entry);
const relative = path.relative(root, entry);
if (relative.startsWith('..' + path.sep) || relative === '..' || path.isAbsolute(relative))
  throw new Error('Playwright resolved outside candidate checkout');
console.log('DEVFLOW_READINESS_ENTRY=' + JSON.stringify(entry));
"""


def probe_browser(broker, requirements, candidate, *, iteration, attempt,
                  dependency_preparation=None):
    """Launch pinned Playwright engines in the actual browser QA boundary.

    This samples the declared engines only. The original browser gate remains
    mandatory, and the caller must re-probe before it runs.
    """
    import json
    import shutil

    from .delivery_native_process import NativeProcess
    from .delivery_preparation import verify_prepared_spec
    from .delivery_readiness_pnpm import authenticate_playwright
    from .delivery_resources import RunResources, private_directory, read_private
    from .delivery_sandbox import prepare_browser_qa, trusted_local

    spec, checkout = broker.spec, broker.checkout
    engines = requirements["declaration"]["browser_engines"]
    qa = requirements["browser_qa"]
    expected = describe_requirements(
        spec, checkout,
        accepted_plan=(spec.get("accepted_plan", "")
                       if requirements.get("accepted_plan_sha256") is not None else None),
    )
    if (not engines or requirements != expected
            or qa != spec["policy"].get("browser_qa")
            or type(iteration) is not int or not 0 <= iteration <= 1000000
            or type(attempt) is not int or not 0 <= attempt <= 1000000
            or broker.candidate() != candidate):
        raise ReadinessError("browser observation needs its exact candidate and frozen recipe")
    if not any(qa["argv"][i:i + 2] == ["playwright", "test"]
               for i in range(len(qa["argv"]) - 1)):
        raise ReadinessError("browser observation only supports an admitted Playwright recipe")
    verify_prepared_spec(spec)
    cwd = (checkout / qa.get("cwd", ".")).resolve(strict=True)
    if checkout not in (cwd, *cwd.parents):
        raise ReadinessError("browser QA working directory left the candidate")
    folder = (Path(spec["state_dir"]) / "readiness" / "browser"
              / str(requirements["declaration"]["generation"]) / str(attempt)
              / str(iteration))
    private_directory(folder)
    resources = RunResources(spec)
    scratch = resources.browser_scratch()
    profile, environment = prepare_browser_qa(spec, checkout, folder, scratch, qa)
    missing = shared_missing_node(broker.store, requirements, candidate, environment)
    if missing is not None:
        return {**missing, "cleanup": "confirmed", "candidate": candidate,
                "requirement_sha256": digest(requirements)}
    resolved = shutil.which("node", path=environment["PATH"])
    if not resolved:
        raise ReadinessError("browser Node disappeared after the shared observation")
    node = _file_reference(str(Path(resolved).resolve(strict=True)))

    def run(name, script, operands):
        command = [node["path"], "-e", script, *operands]
        argv = (command if trusted_local(spec)
                else ["/usr/bin/sandbox-exec", "-f", str(profile), *command])
        native = NativeProcess(spec, folder / name, argv=argv, cwd=cwd,
                               environment=environment, timeout=60,
                               cancelled=broker._native_cancelled)
        result = broker._run_native_check(native)
        if result.get("cleanup") != "observed-native-confirmed":
            return result, None, None
        if broker.candidate() != candidate or node != _file_reference(node["path"]):
            raise ReadinessError("browser observation source or Node executable changed")
        journal = read_private(native.journal)
        if (journal.get("phase") != "finished" or journal.get("result") != result
                or journal.get("intent") != native._intent()):
            raise ReadinessError("browser observation lost its native process custody")
        log = Path(result["log"])
        if (log.parent != native.folder or log.is_symlink() or log.stat().st_size > 1024 * 1024
                or hashlib.sha256(log.read_bytes()).hexdigest() != result["log_sha256"]):
            raise ReadinessError("browser observation log changed")
        raw = read_private(native.journal, raw=True)
        evidence = [{"path": str(native.journal), "sha256": hashlib.sha256(raw).hexdigest()},
                    {"path": str(log), "sha256": result["log_sha256"]}]
        return result, log, evidence

    def failure(result, evidence):
        cleanup = ("confirmed" if result.get("cleanup") == "observed-native-confirmed"
                   else "unknown")
        state = ("unknown" if cleanup == "unknown" else "cancelled" if result.get("cancelled")
                 else "unknown" if (result.get("timed_out") or result.get("listener_conflict")
                     or not result.get("monitoring_complete") or not result.get("stdio_drained"))
                 else "failed")
        return {"state": state, "cleanup": cleanup, "component": "browser",
                "reason": "isolated browser probe did not complete successfully",
                "native_process": result, "launched": result.get("started_at") is not None,
                "candidate": candidate, "requirement_sha256": digest(requirements),
                "evidence": evidence or []}

    def completed(result, log):
        return (log is not None and result.get("exit_code") == 0
                and result.get("monitoring_complete") is True
                and result.get("stdio_drained") is True
                and all(result.get(flag) is False
                        for flag in ("cancelled", "timed_out", "listener_conflict")))

    resolution, log, resolution_evidence = run("resolve-native", _BROWSER_RESOLVE, [str(checkout)])
    if not completed(resolution, log):
        return failure(resolution, resolution_evidence)
    prefix = "DEVFLOW_READINESS_ENTRY="
    lines = [line[len(prefix):] for line in log.read_text(errors="strict").splitlines()
             if line.startswith(prefix)]
    if len(lines) != 1:
        raise ReadinessError("browser resolution did not return one candidate package")
    entry = json.loads(lines[0])
    before = authenticate_playwright(spec, checkout, dependency_preparation, entry, [entry])
    result, log, evidence = run("browser-native", _BROWSER_PROBE,
                                [str(checkout), json.dumps(engines)])
    if not completed(result, log):
        return failure(result, evidence)
    prefix = "DEVFLOW_READINESS_BROWSER="
    lines = [line[len(prefix):] for line in log.read_text(errors="strict").splitlines()
             if line.startswith(prefix)]
    if len(lines) != 1:
        raise ReadinessError("browser observation did not return one engine inventory")
    output = json.loads(lines[0])
    if (not isinstance(output, dict) or set(output) != {"entry", "executables", "modules"}
            or output["entry"] != entry or not isinstance(output["executables"], dict)
            or set(output["executables"]) != set(engines)):
        raise ReadinessError("browser observation returned an inconsistent engine inventory")
    after = authenticate_playwright(
        spec, checkout, dependency_preparation, entry, output["modules"])
    if before["packages"] != after["packages"]:
        raise ReadinessError("browser package closure changed during its probe")
    binaries = {name: _file_reference(path) for name, path in output["executables"].items()}
    return {"state": "passed", "cleanup": "confirmed", "component": "browser",
            "node": node, "package_entry": _file_reference(entry, maximum=16 * 1024 * 1024),
            "binaries": binaries, "package_proof": after, "modules": output["modules"],
            "dependency_preparation": dependency_preparation,
            "native_process": result, "resolution_process": resolution,
            "profile_sha256": hashlib.sha256(profile.read_bytes()).hexdigest(),
            "environment_sha256": digest(environment), "candidate": candidate,
            "requirement_sha256": digest(requirements),
            "evidence": resolution_evidence + evidence}

def shared_missing_node(store, requirements, candidate, environment):
    """Share an observed missing Node executable, rechecking absence on every use.

    This records a refusal only. A present Node invalidates it and cannot grant
    execution. The caller still needs the complete per-run readiness proof.
    """
    import json
    import shutil

    from .delivery_preparation import _lock, _private_bytes, _write

    path_value = environment.get("PATH")
    if (not isinstance(path_value, str) or not path_value or len(path_value) > 16384
            or any(not value or not Path(value).is_absolute() for value in path_value.split(":"))):
        raise ReadinessError("browser environment PATH is not a bounded absolute recipe")
    key = {"version": _SCHEMA, "component": "browser-node",
           "requirements_sha256": digest(requirements), "candidate": candidate,
           "path": path_value, "generation": requirements["declaration"]["generation"]}
    root = Path(store.config.state_root) / "readiness-failures"
    with _lock(root):
        file = root / (digest(key) + ".json")
        if shutil.which("node", path=path_value) is not None:
            # Retain old evidence as a historical observation. It cannot block
            # the newly observed environment once the missing tool appears.
            return None
        payload = {"version": _SCHEMA, "key": key, "state": "failed",
                   "component": "browser", "reason": "node unavailable in browser environment",
                   "launched": False}
        if os.path.lexists(file):
            raw = _private_bytes(file, root)
            try:
                prior = json.loads(raw)
            except (ValueError, UnicodeError) as exc:
                raise ReadinessError("shared readiness failure evidence is malformed") from exc
            if prior != payload:
                raise ReadinessError("shared readiness failure evidence changed")
            reused = True
        else:
            if sum(1 for _ in root.glob("*.json")) >= 512:
                raise ReadinessError("shared readiness failure inventory is full")
            _write(file, payload)
            raw = _private_bytes(file, root)
            reused = False
        return {**payload, "reused": reused, "evidence": str(file),
                "evidence_sha256": hashlib.sha256(raw).hexdigest()}



def _python_environment_fingerprint(spec, project, preparation, *, workspace=None):
    """Bind the owned installed graph, including symlink targets and interpreter."""
    from .contracts import canonical_json

    proof = {"declaration": {"python_project": project}}
    interpreter = _temporal_interpreter(spec, proof, preparation=preparation, workspace=workspace)
    root = interpreter.parent.parent
    inventory, total = {}, 0
    for path in sorted(root.rglob("*")):
        if len(inventory) >= 50000:
            raise ReadinessError("Python environment inventory is unbounded")
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            resolved = path.resolve(strict=True)
            if not resolved.is_relative_to(root) and not (
                    path.parent.name == "bin" and re.fullmatch(r"python(?:3(?:\.\d+)?)?", path.name)
                    and resolved.is_file()):
                raise ReadinessError("Python environment contains an unowned symlink")
            inventory[relative] = {"link": os.readlink(path), "target": str(resolved)}
            if not resolved.is_relative_to(root):
                inventory[relative]["target_sha256"] = _file_reference(str(resolved))["sha256"]
            continue
        if path.is_dir():
            continue
        if not path.is_file():
            raise ReadinessError("Python environment contains unsupported data")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size > 256 * 1024 * 1024:
                raise ReadinessError("Python environment file is not bounded regular data")
            total += before.st_size
            if total > 2 * 1024 * 1024 * 1024:
                raise ReadinessError("Python environment exceeds its byte bound")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                value = hashlib.file_digest(stream, "sha256").hexdigest()
            after, named = os.fstat(fd), path.lstat()
            fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
            if (any(getattr(before, key) != getattr(after, key) for key in fields)
                    or (before.st_dev, before.st_ino) != (named.st_dev, named.st_ino)):
                raise ReadinessError("Python environment changed while measured")
            inventory[relative] = {"sha256": value, "mode": stat.S_IMODE(before.st_mode)}
        finally:
            os.close(fd)
    if not inventory:
        raise ReadinessError("Python environment inventory is empty")
    real = _file_reference(str(interpreter.resolve(strict=True)))
    return {"project": project, "root": str(root), "interpreter": str(interpreter),
            "interpreter_sha256": real["sha256"], "files": len(inventory),
            "content_sha256": hashlib.sha256(canonical_json(inventory).encode()).hexdigest()}


def _native_fetch_allowed(spec, requirements):
    """Do not add registry access that no frozen prerequisite admitted."""
    if spec.get("policy", {}).get("host_sandbox") == "trusted-local":
        return True
    for check in requirements["configured"]:
        if "/store" not in check.get("argv", []):
            continue
        domains = check.get("network_domains", [])
        if not isinstance(domains, list) or not any(
            domain == "registry.npmjs.org" for domain in domains
        ):
            return False
    return True

def probe_configured(broker, requirements, candidate, *, attempt):
    """Execute frozen prerequisite recipes before scheduling expensive roles.

    These are prerequisite observations. They do not replace any configured
    baseline, prepublication, local, or browser gate.
    """
    from .delivery_broker import CheckPreparationFailure
    from .delivery_preparation import verify_prepared_spec

    spec, checkout = broker.spec, broker.checkout
    expected = describe_requirements(spec, checkout)
    if (requirements != expected
            or type(attempt) is not int or not 0 <= attempt <= 1000000
            or broker.candidate() != candidate):
        raise ReadinessError("configured preparation lost its admitted candidate or recipes")
    verify_prepared_spec(spec)
    if not _native_fetch_allowed(spec, requirements):
        return {"state": "unknown", "cleanup": "confirmed", "component": "configured",
                "candidate": candidate, "requirement_sha256": digest(requirements),
                "reason": "frozen prerequisite did not admit registry network access"}
    broker._prepare_generated_outputs(checkout)
    recipes = requirements["configured"]
    folder = broker.evidence_dir / "implementation-preparation" / ("configured-" + str(attempt))
    try:
        result = (broker._run_check_list(checkout, recipes, folder, candidate,
                                         preserve_workspace_dependencies=True)
                  if recipes else {"state": "passed", "results": [],
                                   "candidate_id": candidate["id"], "source_unchanged": True})
    except CheckPreparationFailure as exc:
        result = {"state": "failed", "results": exc.results, "diagnostic": str(exc),
                  "candidate_id": candidate["id"],
                  "source_unchanged": broker.candidate() == candidate}
    if broker.candidate() != candidate or result.get("candidate_id") != candidate["id"]:
        raise ReadinessError("configured preparation changed its candidate")
    if result.get("source_unchanged") is not True:
        raise ReadinessError("configured preparation lost source custody")
    cleanup = ("unknown" if result.get("cleanup") == "unknown"
               or not broker.native_cleanup_confirmed
               or any(item.get("cleanup") != "confirmed" for item in result.get("results", []))
               else "confirmed")
    projects = sorted({str(Path(name).parent) for check in recipes
                       for name in check.get("generated_directories", [])
                       if Path(name).name == ".venv"})
    environments = ([_python_environment_fingerprint(spec, project, result, workspace=checkout)
                     for project in projects]
                    if result.get("state") == "passed" and cleanup == "confirmed" else [])
    return {"state": result.get("state") if cleanup == "confirmed" else "unknown",
            "cleanup": cleanup, "component": "configured", "candidate": candidate,
            "requirement_sha256": digest(requirements), "result": result,
            "python_environments": environments}



def plan_evidence_folder(spec, evidence_root, iteration, candidate, attempt):
    """Separate new observations from all historical native attempt journals."""
    identity = candidate.get("id") if isinstance(candidate, dict) else None
    if (spec.get("readiness_version") != 1 or type(iteration) is not int or iteration < 0
            or type(attempt) is not int or not 0 <= attempt <= 1000000
            or not isinstance(identity, str) or not re.fullmatch(r"[0-9a-f]{64}", identity)):
        raise ReadinessError("plan readiness attempt has no exact candidate authority")
    return (Path(evidence_root) / "implementation-preparation" / str(iteration)
            / ("readiness-" + str(attempt) + "-" + identity))

def probe_plan(broker, requirements, candidate, *, iteration, attempt):
    """Prepare locked dependencies for the exact accepted plan before a new role."""
    from .delivery_broker import CheckPreparationFailure
    from .delivery_preparation import verify_prepared_spec

    spec, checkout = broker.spec, broker.checkout
    expected = describe_requirements(spec, checkout, accepted_plan=spec.get("accepted_plan", ""))
    if (requirements != expected
            or type(iteration) is not int or not 0 <= iteration <= 1000000
            or broker.candidate() != candidate):
        raise ReadinessError("plan preparation lost its admitted candidate or recipes")
    verify_prepared_spec(spec)
    try:
        _planned_tool_recipes(broker, candidate, iteration, attempt, None)
    except (OSError, ValueError) as exc:
        return {"state": "unknown", "cleanup": "confirmed", "component": "plan",
                "candidate": candidate, "requirement_sha256": digest(requirements),
                "reason": "planned check inventory could not be authenticated",
                "error_type": type(exc).__name__}
    if not _native_fetch_allowed(spec, requirements):
        return {"state": "unknown", "cleanup": "confirmed", "component": "plan",
                "candidate": candidate, "requirement_sha256": digest(requirements),
                "reason": "frozen prerequisite did not admit registry network access"}
    try:
        result = broker.run_implementation_preparation(
            iteration, candidate, readiness_attempt=attempt)
    except CheckPreparationFailure as exc:
        result = {"state": "failed", "results": exc.results, "diagnostic": str(exc),
                  "candidate_id": candidate["id"],
                  "source_unchanged": broker.candidate() == candidate}
    if broker.candidate() != candidate or result.get("candidate_id") != candidate["id"]:
        raise ReadinessError("plan preparation changed its candidate")
    if result.get("source_unchanged") is not True:
        raise ReadinessError("plan preparation lost source custody")
    cleanup = ("unknown" if result.get("cleanup") == "unknown"
               or not broker.native_cleanup_confirmed
               or any(item.get("cleanup") != "confirmed" for item in result.get("results", []))
               else "confirmed")
    from .delivery_plan_checks import planned_checks

    folder = plan_evidence_folder(spec, broker.evidence_dir, iteration, candidate, attempt)
    planned = planned_checks(spec, checkout, folder, preparation=True)
    projects = {check["cwd"] for check in planned
                if check["id"].startswith("planned-python-dependencies-")}
    projects.update(str(Path(name).parent) for check in requirements["configured"]
                    for name in check.get("generated_directories", [])
                    if Path(name).name == ".venv")
    environments = ([_python_environment_fingerprint(spec, project, result, workspace=checkout)
                     for project in sorted(projects)]
                    if result.get("state") == "passed" and cleanup == "confirmed" else [])
    return {"state": result.get("state") if cleanup == "confirmed" else "unknown",
            "cleanup": cleanup, "component": "plan", "candidate": candidate,
            "requirement_sha256": digest(requirements), "result": result,
            "python_environments": environments}



_TEMPORAL_PROBE = r"""
import asyncio, importlib.metadata, json, os
from temporalio.testing import WorkflowEnvironment
from temporalio.api.workflowservice.v1 import GetSystemInfoRequest

async def main():
    version = importlib.metadata.version('temporalio')
    if version != '1.26.0':
        raise RuntimeError('locked Temporal SDK version does not match admitted tools')
    server = os.environ['JOBCTRL_TEMPORAL_TEST_SERVER_PATH']
    cli = os.environ['JOBCTRL_TEMPORAL_CLI_PATH']
    first = await WorkflowEnvironment.start_time_skipping(test_server_existing_path=server)
    try:
        await first.client.service_client.workflow_service.get_system_info(GetSystemInfoRequest())
    finally:
        await first.shutdown()
    second = await WorkflowEnvironment.start_local(dev_server_existing_path=cli)
    try:
        await second.client.service_client.workflow_service.get_system_info(GetSystemInfoRequest())
    finally:
        await second.shutdown()
    print('DEVFLOW_READINESS_TEMPORAL=' + json.dumps({'sdk': version, 'server': server, 'cli': cli},
                                                     sort_keys=True))
asyncio.run(asyncio.wait_for(main(), timeout=150))
"""


def _temporal_interpreter(spec, proof, *, preparation=None, requested=None, workspace=None):
    from .delivery_resources import RunResources, same_object

    state = Path(spec["state_dir"])
    checkout = Path(workspace) if workspace is not None else Path(spec["checkout"])
    project = proof["declaration"]["python_project"]
    matches = [item.get("interpreter")
               for item in (preparation or {}).get("python_interpreters", [])
               if item.get("project") == project]
    if len(matches) > 1:
        raise ReadinessError("Temporal preparation has conflicting Python environments")
    interpreter = Path(requested or (matches[0] if matches
                                     else checkout / project / ".venv/bin/python"))
    if interpreter.name != "python" or interpreter.parent.name != "bin":
        raise ReadinessError("Temporal readiness needs the owned locked Python interpreter")
    envroot = interpreter.parent.parent
    if (not envroot.is_absolute() or envroot.resolve(strict=True) != envroot
            or not interpreter.is_file() or not (envroot / "pyvenv.cfg").is_file()):
        raise ReadinessError("Temporal Python environment is unavailable or aliased")
    resources = RunResources(spec, read_only=True)
    with resources.locked(read_only=True) as manifest:
        roots = manifest.get("roots", {})
        if envroot == checkout / project / ".venv":
            owner = roots.get(str(envroot), {})
            if (owner.get("kind") != "generated" or not owner.get("identity")
                    or not same_object(spec, envroot, owner["identity"])):
                raise ReadinessError("Temporal Python environment is not controller-owned")
        elif envroot.is_relative_to(state / "transient/implementation-python"):
            transient = state / "transient"
            owner = roots.get(str(transient), {})
            if (owner.get("kind") != "transient" or not owner.get("identity")
                    or not same_object(spec, transient, owner["identity"])):
                raise ReadinessError("Temporal isolated Python environment lost its owner")
        else:
            raise ReadinessError("Temporal Python environment is outside admitted ownership")
    return interpreter


def temporal_launch_binding(process, proof, authority):
    """Admit only the fixed controller probe under the exact copied-tool owner."""
    interpreter = _temporal_interpreter(process.spec, proof, requested=process.argv[0],
                                        workspace=authority["subject"]["path"])
    argv = [str(interpreter), "-I", "-B", "-c", _TEMPORAL_PROBE]
    if (process.spec["policy"].get("host_sandbox") != "trusted-local"
            or process.argv != argv or not process.folder.name.startswith("readiness-native-")
            or not process.folder.name.removeprefix("readiness-native-").isdecimal()):
        raise ReadinessError("Temporal readiness launch differs from its fixed recipe")
    return argv, 180, {process.folder.name}


def probe_temporal_preparation(broker, requirements, candidate, *, iteration, attempt=0,
                               preparation=None):
    """Start the admitted pinned server and CLI in the exact test environment."""
    import json

    from .delivery_native_process import NativeProcess
    from .delivery_preparation import verify_prepared_spec
    from .delivery_resources import read_private
    from .delivery_sandbox import prepare_native_check
    from .delivery_temporal_prerequisites import MARKER, launch_authority

    spec, checkout = broker.spec, broker.checkout
    plan = (spec.get("accepted_plan", "")
            if requirements.get("accepted_plan_sha256") is not None else None)
    expected = describe_requirements(spec, checkout, accepted_plan=plan)
    declared = requirements["declaration"]["temporal_checks"]
    if (requirements != expected or not declared
            or type(iteration) is not int or not 0 <= iteration <= 1000000
            or type(attempt) is not int or not 0 <= attempt <= 1000000
            or broker.candidate() != candidate):
        raise ReadinessError("Temporal observation lost its admitted candidate or recipes")
    fresh = (spec.get("readiness_version") == 1
             and requirements["declaration"].get("temporal_provenance"))
    if not spec.get(MARKER) and not fresh:
        return {"state": "failed", "cleanup": "confirmed", "component": "temporal",
                "reason": "declared Temporal test lacks an admitted tool binding",
                "candidate": candidate, "requirement_sha256": digest(requirements),
                "launched": False}
    verify_prepared_spec(spec)
    proof = launch_authority(spec)
    check = proof["check"]
    if declared != [check["id"]]:
        raise ReadinessError("Temporal observation differs from its admitted exact check")
    folder = broker.evidence_dir / "checks" / str(iteration)
    profile, environment = prepare_native_check(spec, checkout, folder, check,
                                                resources=broker.run_resources())
    interpreter = _temporal_interpreter(spec, proof, preparation=preparation, workspace=checkout)
    cwd = (checkout / check.get("cwd", ".")).resolve(strict=True)
    process = NativeProcess(spec, folder / check["id"] / ("readiness-native-" + str(attempt)),
                            argv=[str(interpreter), "-I", "-B", "-c", _TEMPORAL_PROBE],
                            cwd=cwd, environment=environment, timeout=180,
                            cancelled=broker._native_cancelled)
    process.readiness_temporal = True
    result = broker._run_native_check(process)
    if result.get("cleanup") != "observed-native-confirmed":
        return {"state": "unknown", "cleanup": "unknown", "component": "temporal",
                "reason": "Temporal readiness teardown is unknown", "native_process": result}
    journal = read_private(process.journal)
    if (journal.get("phase") != "finished" or journal.get("intent") != process._intent()
            or journal.get("result") != result or broker.candidate() != candidate):
        raise ReadinessError("Temporal readiness lost its process or candidate custody")
    log = Path(result["log"])
    raw = read_private(log, raw=True)
    if (log != process.folder / "process.log" or len(raw) > 1024 * 1024
            or hashlib.sha256(raw).hexdigest() != result.get("log_sha256")):
        raise ReadinessError("Temporal readiness log changed")
    if (result.get("exit_code") != 0 or result.get("cancelled") or result.get("timed_out")
            or result.get("listener_conflict") or not result.get("monitoring_complete")
            or not result.get("stdio_drained")):
        state = ("cancelled" if result.get("cancelled")
                 else "unknown" if result.get("timed_out") else "failed")
        return {"state": state, "cleanup": "confirmed", "component": "temporal",
                "reason": "Temporal server or CLI readiness failed", "native_process": result,
                "candidate": candidate, "requirement_sha256": digest(requirements),
                "evidence": [{"path": str(process.journal), "sha256": hashlib.sha256(
                    read_private(process.journal, raw=True)).hexdigest()},
                    {"path": str(log), "sha256": result["log_sha256"]}]}
    prefix = "DEVFLOW_READINESS_TEMPORAL="
    lines = [line[len(prefix):] for line in raw.decode().splitlines() if line.startswith(prefix)]
    expected_output = {"sdk": "1.26.0", "server": environment["JOBCTRL_TEMPORAL_TEST_SERVER_PATH"],
                       "cli": environment["JOBCTRL_TEMPORAL_CLI_PATH"]}
    if len(lines) != 1 or json.loads(lines[0]) != expected_output:
        raise ReadinessError("Temporal readiness did not confirm both admitted backends")
    return {"state": "passed", "cleanup": "confirmed", "component": "temporal",
            "candidate": candidate, "requirement_sha256": digest(requirements),
            "interpreter": str(interpreter), "interpreter_sha256": _file_reference(
                str(interpreter.resolve(strict=True)))["sha256"],
            "environment_sha256": digest(environment), "native_process": result,
            "python_environment": _python_environment_fingerprint(
                spec, proof["declaration"]["python_project"], preparation, workspace=checkout),
            "profile": profile, "preparation": preparation}

def resource_binding(spec, *, workspace=None):
    """Read actual owned resource identities and generations under their manifest lock."""
    from .delivery_resources import RunResources, same_object

    resources = RunResources(spec, read_only=True)
    state = Path(spec["state_dir"])
    required = {Path(spec["checkout"]), state / "transient"}
    if workspace is not None:
        required.add(Path(workspace))
    observed = {}
    with resources.locked(read_only=True) as manifest:
        roots = manifest.get("roots")
        if not isinstance(roots, dict):
            raise ReadinessError("readiness resource inventory is unavailable")
        for raw, entry in roots.items():
            if not isinstance(raw, str) or not isinstance(entry, dict):
                raise ReadinessError("readiness resource inventory is malformed")
            path = Path(raw)
            if path not in required and entry.get("kind") not in {
                "browser-scratch", "execution-scratch"
            }:
                continue
            if (not isinstance(entry, dict) or entry.get("kind") not in {
                    "checkout", "gate", "transient", "browser-scratch", "execution-scratch"
                } or not entry.get("identity")
                    or entry.get("receipt", {}).get("state") in {"removed", "already_absent"}
                    or type(entry.get("generation", 0)) is not int
                    or entry.get("generation", 0) < 0):
                raise ReadinessError("readiness resource owner is missing or settled")
            resources._allowed(path, entry["kind"])
            if not same_object(spec, path, entry["identity"]):
                raise ReadinessError("readiness resource identity changed")
            observed[raw] = {"kind": entry["kind"], "identity": entry["identity"],
                             "generation": entry.get("generation", 0)}
        if not {str(path) for path in required} <= observed.keys():
            raise ReadinessError("readiness checkout or transient resource is not owned")
    return observed



def _validate_workspace(spec, workspace, candidate, iteration):
    from .delivery_resources import RunResources, _gate_path, same_object

    if not workspace.is_absolute() or workspace.resolve(strict=True) != workspace:
        raise ReadinessError("readiness workspace is not canonical")
    if workspace == Path(spec["checkout"]):
        if not _candidate_matches(workspace, candidate):
            raise ReadinessError("readiness checkout candidate changed")
        return
    if workspace != _gate_path(spec, "verify", iteration):
        raise ReadinessError("readiness workspace is not the admitted verify gate")
    resources = RunResources(spec, read_only=True)
    with resources.locked(read_only=True) as manifest:
        entry = manifest.get("roots", {}).get(str(workspace), {})
        if (entry.get("kind") != "gate" or not entry.get("identity")
                or entry.get("head") != candidate.get("head")
                or not same_object(spec, workspace, entry["identity"])
                or not _candidate_matches(workspace, candidate)):
            raise ReadinessError("readiness verify gate lost candidate or resource custody")


def _tool_command(check, cwd, environment):
    import shutil

    argv = check.get("argv")
    if not isinstance(argv, list) or not argv or not isinstance(argv[0], str):
        raise ReadinessError("check executable declaration is malformed")
    raw = argv[0]
    name = Path(raw).name
    known = {"node", "uv", "git", "corepack", "npm", "pnpm", "pytest", "ruff",
             "vitest", "eslint", "tsc", "npx"}
    if name not in known and not re.fullmatch(r"python(?:3(?:\.\d+)?)?", name):
        return None
    if "/" in raw and not Path(raw).is_absolute():
        if ".." in Path(raw).parts:
            return None
        resolved = str(cwd / raw)
    else:
        resolved = raw if Path(raw).is_absolute() else shutil.which(raw, path=environment["PATH"])
    if not resolved or not Path(resolved).is_file() or not os.access(resolved, os.X_OK):
        return None
    if name == "corepack":
        if len(argv) < 4 or argv[1] != "pnpm" or argv[2] != "exec" or argv[3] not in known:
            return None
        return [resolved, "pnpm", "exec", argv[3], "--version"]
    if name in {"pnpm", "npm", "npx", "uv"} and len(argv) > 1:
        return None  # An opaque package-manager script is not a dependency proof.
    if re.fullmatch(r"python(?:3(?:\.\d+)?)?", name) and len(argv) > 2 and argv[1] == "-m":
        if argv[2] not in {"pytest", "ruff"}:
            return None
        return [resolved, "-I", "-B", "-m", argv[2], "--version"]
    if re.fullmatch(r"python(?:3(?:\.\d+)?)?", name) and len(argv) > 1:
        if argv[1] not in {"--version", "-V"}:
            return None  # Arbitrary scripts cannot prove their dependency closure.
    if name == "node" and len(argv) > 1 and argv[1] not in {"--test", "--version", "-v"}:
        return None
    return [resolved, "--version"]


def temporal_tool_launch_binding(process, proof, authority):
    """Preserve exact Temporal tool custody for a bounded executable probe."""
    command = _tool_command(proof["check"], process.cwd, process.environment)
    if (command is None or process.argv != command
            or not process.folder.name.startswith("readiness-tool-")
            or not process.folder.name.removeprefix("readiness-tool-").isdecimal()
            or not process.cwd.is_relative_to(Path(authority["subject"]["path"]))):
        raise ReadinessError("Temporal tool probe differs from its fixed check executable")
    return command, 60, {process.folder.name}



def _node_environment_fingerprint(spec, workspace):
    """Bind the owned installed Node dependency graph, including internal aliases."""
    from .contracts import canonical_json
    from .delivery_resources import RunResources, same_object

    root = Path(workspace) / "node_modules"
    resources = RunResources(spec, read_only=True)
    with resources.locked(read_only=True) as manifest:
        owner = manifest.get("roots", {}).get(str(root), {})
        if (owner.get("kind") != "generated" or not owner.get("identity")
                or not same_object(spec, root, owner["identity"])):
            raise ReadinessError("Node dependency environment is not controller-owned")
    inventory, total = {}, 0
    for path in sorted(root.rglob("*")):
        if len(inventory) >= 100000:
            raise ReadinessError("Node dependency inventory is unbounded")
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            resolved = path.resolve(strict=True)
            if not resolved.is_relative_to(Path(workspace)):
                raise ReadinessError("Node dependency alias escaped its candidate")
            inventory[relative] = {"link": os.readlink(path), "target": str(resolved)}
            continue
        if path.is_dir():
            continue
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            before = os.fstat(fd)
            if not stat.S_ISREG(before.st_mode) or before.st_size > 256 * 1024 * 1024:
                raise ReadinessError("Node dependency file is not bounded regular data")
            total += before.st_size
            if total > 4 * 1024 * 1024 * 1024:
                raise ReadinessError("Node dependency inventory exceeds its byte bound")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                value = hashlib.file_digest(stream, "sha256").hexdigest()
            after, named = os.fstat(fd), path.lstat()
            fields = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
            if (any(getattr(before, key) != getattr(after, key) for key in fields)
                    or (before.st_dev, before.st_ino) != (named.st_dev, named.st_ino)):
                raise ReadinessError("Node dependency file changed while measured")
            inventory[relative] = {"sha256": value, "mode": stat.S_IMODE(before.st_mode)}
        finally:
            os.close(fd)
    if not inventory:
        raise ReadinessError("Node dependency environment inventory is empty")
    return {"root": str(root), "files": len(inventory),
            "content_sha256": hashlib.sha256(canonical_json(inventory).encode()).hexdigest()}

def _planned_tool_recipes(broker, candidate, iteration, attempt, preparation):
    """Regenerate plan-selected checks with their exact prepared interpreter."""
    from .delivery_plan_checks import planned_checks

    folder = plan_evidence_folder(broker.spec, broker.evidence_dir, iteration, candidate, attempt)
    planned = planned_checks(broker.spec, broker.checkout, folder, preparation=True)
    if len(planned) > 128:
        raise ReadinessError("planned check inventory is unbounded")
    interpreters = {item["project"]: item["interpreter"]
                    for item in (preparation or {}).get("result", {}).get(
                        "python_interpreters", [])}
    recipes = []
    for check in planned:
        if check.get("id", "").startswith("planned-python-dependencies-"):
            continue
        check = dict(check)
        if check["id"].startswith("planned-pytest-") and check["cwd"] in interpreters:
            check["argv"] = [interpreters[check["cwd"]], *check["argv"][1:]]
        recipes.append({"stage": "plan-checks", "check": check})
    return recipes


def probe_tools(broker, requirements, candidate, *, iteration, attempt, preparation=None):
    """Exercise supported check executables in their actual native check profiles."""
    from .delivery_native_process import NativeProcess
    from .delivery_sandbox import native_check_argv, prepare_native_check

    spec, checkout = broker.spec, broker.checkout
    rows = []
    node_graph = None
    configured = {digest(check) for check in requirements["configured"]}
    temporal_ids = set(requirements["declaration"]["temporal_checks"])
    recipes = list(requirements["check_recipes"])
    if requirements.get("accepted_plan_sha256") is not None:
        recipes.extend(_planned_tool_recipes(broker, candidate, iteration, attempt, preparation))
    for item in recipes:
        stage, check = item["stage"], item["check"]
        if digest(check) in configured:
            continue  # The unchanged configured recipe already ran and was authenticated.
        if check.get("id") in temporal_ids and stage != "checks":
            continue  # The identical frozen test is checked at its admitted checks subject.
        evidence = broker.evidence_dir / stage / str(iteration)
        cwd = (checkout / check.get("cwd", ".")).resolve(strict=True)
        if checkout not in (cwd, *cwd.parents):
            raise ReadinessError("check executable working directory escaped its workspace")
        profile, environment = prepare_native_check(spec, checkout, evidence, check,
                                                    resources=broker.run_resources(),
                                                    readiness_probe=stage == "plan-checks")
        # Do not let a version probe fetch a missing package-manager executable.
        environment["COREPACK_ENABLE_NETWORK"] = "0"
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        command = _tool_command(check, cwd, environment)
        if command is None:
            return {"state": "unknown", "cleanup": "confirmed", "component": "tools",
                    "reason": "configured check executable is missing or unsupported",
                    "check_id": check.get("id"), "candidate": candidate,
                    "requirement_sha256": digest(requirements), "items": rows}
        name = Path(check["argv"][0]).name
        python_module = (name in {"pytest", "ruff"} or
                         re.fullmatch(r"python(?:3(?:\.\d+)?)?", name)
                         and len(check["argv"]) > 2 and check["argv"][1] == "-m")
        if python_module:
            roots = [Path(value["root"]) for value in
                     (preparation or {}).get("python_environments", [])]
            if not any(Path(command[0]).is_relative_to(root / "bin") for root in roots):
                return {"state": "unknown", "cleanup": "confirmed", "component": "tools",
                        "reason": "Python check executable is not in an owned locked environment",
                        "check_id": check.get("id"), "candidate": candidate,
                        "requirement_sha256": digest(requirements), "items": rows}
        needs_node_graph = (name in {"vitest", "eslint", "tsc"}
                            or name == "node" and len(check["argv"]) > 1
                            and check["argv"][1] == "--test"
                            or name == "corepack")
        if needs_node_graph and node_graph is None:
            try:
                node_graph = _node_environment_fingerprint(spec, checkout)
            except (OSError, ValueError):
                return {"state": "unknown", "cleanup": "confirmed", "component": "tools",
                        "reason": "Node dependency environment is unavailable or unverified",
                        "check_id": check["id"], "candidate": candidate,
                        "requirement_sha256": digest(requirements), "items": rows}
        executable = _file_reference(str(Path(command[0]).resolve(strict=True)))
        argv = native_check_argv(spec, profile, cwd, command)
        folder = evidence / check["id"] / ("readiness-tool-" + str(attempt))
        process = NativeProcess(spec, folder, argv=argv, cwd=cwd, environment=environment,
                                timeout=60, cancelled=broker._native_cancelled)
        if check.get("id") in temporal_ids:
            process.readiness_tool = True
        result = broker._run_native_check(process)
        cleanup = ("confirmed" if result.get("cleanup") == "observed-native-confirmed"
                   else "unknown")
        if (cleanup != "confirmed" or result.get("exit_code") != 0
                or result.get("cancelled") or result.get("timed_out")
                or result.get("listener_conflict") or not result.get("monitoring_complete")
                or not result.get("stdio_drained")):
            return {"state": "unknown" if cleanup == "unknown" else "failed",
                    "cleanup": cleanup, "component": "tools", "candidate": candidate,
                    "reason": "configured check executable probe failed", "check_id": check["id"],
                    "requirement_sha256": digest(requirements), "native_process": result,
                    "items": rows}
        if (broker.candidate() != candidate
                or executable != _file_reference(str(Path(command[0]).resolve(strict=True)))):
            raise ReadinessError("check executable or candidate changed during its probe")
        rows.append({"stage": stage, "check_id": check["id"], "recipe_sha256": digest(check),
                     "executable": executable, "native_process": result})
    return {"state": "passed", "cleanup": "confirmed", "component": "tools",
            "candidate": candidate, "requirement_sha256": digest(requirements), "items": rows,
            "node_environment": node_graph}

def _browser_node_before_preparation(broker, requirements, candidate, *, iteration, attempt):
    """Cheap shared failure observation before any configured dependency launch."""
    from .delivery_resources import RunResources, private_directory
    from .delivery_sandbox import prepare_browser_qa

    qa = requirements["browser_qa"]
    if not qa:
        return None
    spec = broker.spec
    folder = (Path(spec["state_dir"]) / "readiness" / "browser"
              / str(requirements["declaration"]["generation"]) / str(attempt)
              / str(iteration))
    private_directory(folder)
    scratch = RunResources(spec).browser_scratch()
    _, environment = prepare_browser_qa(spec, broker.checkout, folder, scratch, qa)
    return shared_missing_node(broker.store, requirements, candidate, environment)


def prepare_and_probe(store, spec, candidate, requirements, stage, *, iteration=0, attempt=0,
                      workspace=None):
    """Observe readiness in the caller's admitted native resource context.

    A successful receipt is authenticated but does not itself grant dispatch.
    Callers hold native resource admission and the final launch fence independently.
    """
    from .delivery_broker import DeliveryBroker
    from .delivery_metadata_recovery import _immutable
    from .delivery_preparation import verify_prepared_spec
    from .delivery_resources import private_directory

    if (spec.get("readiness_version") != 1 or stage not in {"configured", "plan"}
            or type(iteration) is not int or not 0 <= iteration <= 1000000
            or type(attempt) is not int or not 0 <= attempt <= 1000000):
        raise ReadinessError("readiness stage is not admitted")
    if store.effective_spec(spec["run_id"]) != spec:
        raise ReadinessError("readiness execution authority changed")
    broker = DeliveryBroker(store, spec)
    selected = Path(workspace) if workspace is not None else broker.checkout
    _validate_workspace(spec, selected, candidate, iteration)
    broker.checkout = selected
    expected = describe_requirements(
        spec, broker.checkout,
        accepted_plan=spec.get("accepted_plan", "") if stage == "plan" else None,
    )
    if requirements != expected or broker.candidate() != candidate:
        raise ReadinessError("readiness stage changed its candidate or requirements")
    verify_prepared_spec(spec)
    # Allocate and bind this run's owned transient generation, never a foreign
    # project .venv. Existing cleanup rules retain exclusive resource custody.
    broker.run_resources().scratch("readiness", "binding")
    observations = {}
    early = _browser_node_before_preparation(
        broker, requirements, candidate, iteration=iteration, attempt=attempt)
    if early is not None:
        observations["browser"] = {**early, "cleanup": "confirmed", "candidate": candidate,
                                   "requirement_sha256": digest(requirements)}
    else:
        observe = probe_configured if stage == "configured" else probe_plan
        observations[stage] = (observe(broker, requirements, candidate, attempt=attempt)
                               if stage == "configured" else
                               observe(broker, requirements, candidate, iteration=iteration,
                                       attempt=attempt))
        if observations[stage]["state"] == "passed":
            try:
                observations["tools"] = probe_tools(
                    broker, requirements, candidate, iteration=iteration, attempt=attempt,
                    preparation=observations[stage])
            except (OSError, ValueError) as exc:
                # A tool or its prerequisite authority may disappear before a
                # native receipt exists. This observation cannot establish cause
                # or cleanup and cannot grant a retry or product launch.
                observations["tools"] = {
                    "state": "unknown", "cleanup": "unknown", "component": "tools",
                    "reason": "Readiness tool authority or preparation could not be verified",
                    "error_type": type(exc).__name__, "candidate": candidate,
                    "requirement_sha256": digest(requirements)}
        if (all(item["state"] == "passed" for item in observations.values())
                and requirements["browser_qa"]):
            dependencies = [item.get("dependency_preparation")
                            for item in observations[stage]["result"].get("results", [])
                            if item.get("dependency_preparation")]
            if len({digest(item) for item in dependencies}) > 1:
                raise ReadinessError("browser prerequisite receipts conflict")
            observations["browser"] = probe_browser(
                broker, requirements, candidate, iteration=iteration, attempt=attempt,
                dependency_preparation=dependencies[0] if dependencies else None)
        if (all(item["state"] == "passed" for item in observations.values())
                and requirements["declaration"]["temporal_checks"]):
            try:
                observations["temporal"] = probe_temporal_preparation(
                    broker, requirements, candidate, iteration=iteration, attempt=attempt,
                    preparation=observations[stage]["result"])
            except (OSError, ValueError) as exc:
                # A failed authority/preparation observation is retained, but an
                # exception alone cannot authenticate a retry cause or cleanup.
                observations["temporal"] = {
                    "state": "unknown", "cleanup": "unknown", "component": "temporal",
                    "reason": "Temporal readiness authority or preparation could not be verified",
                    "error_type": type(exc).__name__, "candidate": candidate,
                    "requirement_sha256": digest(requirements)}
    if broker.candidate() != candidate:
        raise ReadinessError("readiness probes changed their candidate")
    _validate_workspace(spec, selected, candidate, iteration)
    resources = resource_binding(spec, workspace=selected)
    transient = resources[str(Path(spec["state_dir"]) / "transient")]
    subject = binding(spec, candidate, requirements, resource_generation=transient["generation"])
    subject["resources"] = resources
    states = {item["state"] for item in observations.values()}
    state = ("cancelled" if "cancelled" in states else "failed" if "failed" in states
             else "unknown")
    receipt = {"version": _SCHEMA, "state": state, "stage": stage, "iteration": iteration,
               "attempt": attempt, "workspace": str(selected), "binding": subject,
               "observations": observations,
               "reason": "readiness evidence is incomplete or not yet authenticated"}
    if states == {"passed"}:
        _verify_observations(store, spec, candidate, requirements, receipt)
        receipt["state"] = "passed"
        receipt["reason"] = None
    path = (Path(spec["state_dir"]) / "readiness" / "receipts" / stage
            / str(iteration) / f"{attempt}.json")
    private_directory(path.parent)
    _immutable(path, receipt)
    return {**receipt, "receipt": str(path),
            "receipt_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def authenticate_missing_node(store, spec, candidate, requirements, evidence):
    """Authenticate a historical missing-Node fact and observe current remediation.

    This is a read-only observation. 'resolved' means only that a Node image is
    presently visible in the frozen QA PATH; it does not authorize a product role,
    establish browser readiness, or skip any mandatory gate.
    """
    import json
    import shutil

    from .delivery_preparation import verify_prepared_spec
    from .delivery_resources import read_private
    from .delivery_sandbox import _native_path

    if store.effective_spec(spec["run_id"]) != spec:
        raise ReadinessError("shared failure execution authority changed")
    checkout = Path(spec["checkout"])
    plan = (spec.get("accepted_plan", "")
            if requirements.get("accepted_plan_sha256") is not None else None)
    if (requirements != describe_requirements(spec, checkout, accepted_plan=plan)
            or not requirements.get("browser_qa") or not _candidate_matches(checkout, candidate)):
        raise ReadinessError("shared failure candidate or browser recipe changed")
    verify_prepared_spec(spec)
    roots = tuple(Path(raw) for raw in spec["policy"].get("toolchain_roots", []))
    if any(not root.is_absolute() or root.resolve(strict=True) != root or not root.is_dir()
           for root in roots):
        raise ReadinessError("shared failure toolchain root changed")
    path_value = _native_path(roots)
    key = {"version": _SCHEMA, "component": "browser-node",
           "requirements_sha256": digest(requirements), "candidate": candidate,
           "path": path_value, "generation": requirements["declaration"]["generation"]}
    root = Path(store.config.state_root) / "readiness-failures"
    path = root / (digest(key) + ".json")
    if (not isinstance(evidence, dict) or set(evidence) != {"path", "sha256"}
            or evidence["path"] != str(path)
            or not isinstance(evidence["sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", evidence["sha256"])
            or not root.is_absolute() or root.resolve(strict=True) != root):
        raise ReadinessError("shared failure evidence does not belong to the current recipe")
    info = root.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o700):
        raise ReadinessError("shared failure evidence root is not private and owned")
    raw = read_private(path, raw=True)
    if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != evidence["sha256"]:
        raise ReadinessError("shared failure evidence changed")
    try:
        prior = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ReadinessError("shared failure evidence is malformed") from exc
    expected = {"version": _SCHEMA, "key": key, "state": "failed",
                "component": "browser", "reason": "node unavailable in browser environment",
                "launched": False}
    if prior != expected:
        raise ReadinessError("shared failure evidence does not authenticate missing Node")
    current = shutil.which("node", path=path_value)
    observed = _file_reference(str(Path(current).resolve(strict=True))) if current else None
    if (not _candidate_matches(checkout, candidate)
            or store.effective_spec(spec["run_id"]) != spec):
        raise ReadinessError("shared failure candidate or authority changed during observation")
    return {"state": "resolved" if observed else "still_missing", "component": "browser",
            "candidate_id": candidate["id"], "requirement_sha256": digest(requirements),
            "historical_evidence": evidence, "node": observed}
