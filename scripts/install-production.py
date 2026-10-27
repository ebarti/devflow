#!/usr/bin/env python3.12
"""Adopt an independent production checkout before exposing registrations.

The journal is outside the source tree. Interrupted provisioning can be replayed;
rollback retains the incomplete owned tree instead of deleting possible run data.
No configuration, execution history, authority receipt or database is rewritten.
"""

from __future__ import annotations

import fcntl
import ctypes
import importlib.util
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_OWNER_FD = None


def load(name, root=ROOT):
    spec = importlib.util.spec_from_file_location(name, root / "scripts" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def rollback_controller(state, target):
    """Authenticate recovery code before importing or touching registrations."""
    paths = load("installation-paths")
    if not state.get("target_identity") or not same_node(state["target_identity"], node(target)):
        raise ValueError("production rollback owner changed; preserve the target and retained backup")
    if paths.source(target)["revision"] != state["revision"]:
        raise ValueError("production rollback source changed; preserve its files and retained backup")
    relative = "scripts/install-rollback.py"
    wanted = run("git", "-C", target, "show", state["revision"] + ":" + relative,
                 stdout=subprocess.PIPE).stdout
    file = target / relative
    if file.read_bytes() != wanted:
        raise ValueError("production rollback helper changed; preserve its files and retained backup")
    spec = importlib.util.spec_from_file_location("owned_install_rollback", file)
    module = importlib.util.module_from_spec(spec)
    exec(compile(wanted, str(file), "exec"), module.__dict__)
    if not same_node(state["target_identity"], node(target)):
        raise ValueError("production rollback owner changed while authenticating recovery")
    return module


def save(path, value):
    load("installation-paths").write_json(path, value)


def node(path):
    return load("installation-paths").node(path)


def same_node(before, current):
    return load("installation-paths").same_node(before, current)


def rename_exclusive(source, destination):
    """Publish an owned directory without replacing even an empty foreign one."""
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        function = library.renamex_np
        function.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
        arguments = [os.fsencode(source), os.fsencode(destination), 4]  # RENAME_EXCL
    elif sys.platform.startswith("linux"):
        function = library.renameat2
        function.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
        arguments = [-100, os.fsencode(source), -100, os.fsencode(destination), 1]
    else:
        raise ValueError("production ownership requires native no-replace directory publication")
    function.restype = ctypes.c_int
    if function(*arguments):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(destination))


def publish_created(journal, state, target):
    if "target_identity" not in state:
        if os.path.lexists(target):
            raise ValueError("unattributed production destination preserved; it is not this install's owner")
        stage = Path(state["stage"]) if state.get("stage") else None
        if stage is not None and os.path.lexists(stage):
            # mkdir can commit before its identity receipt. That outcome is
            # explicitly unknown: preserve it, never claim or delete its bytes.
            state.setdefault("unknown_allocations", []).append(str(stage))
            stage = None
        if stage is None:
            stage = target.parent / (".production-adopt-" + uuid.uuid4().hex)
        state.update(stage=str(stage), phase="allocating")
        save(journal, state)
        stage.mkdir(mode=0o700)
        state["target_identity"] = node(stage)
        save(journal, state)
    stage = Path(state["stage"])
    if os.path.lexists(target):
        if not same_node(state["target_identity"], node(target)):
            raise ValueError("foreign production destination preserved during publication")
    elif stage.exists() and same_node(state["target_identity"], node(stage)):
        rename_exclusive(stage, target)
    else:
        raise ValueError("production allocation outcome is unknown; retain its journal for reconciliation")
    state["phase"] = "cloning"
    save(journal, state)


def run(*args, **kwargs):
    if args and args[0] == "git" and "env" not in kwargs:
        kwargs["env"] = load("installation-paths").git_environment()
    if _OWNER_FD is not None:
        kwargs["pass_fds"] = (*kwargs.get("pass_fds", ()), _OWNER_FD)
    return subprocess.run(list(map(str, args)), check=True, **kwargs)


def stopped(target):
    for name, args in (("delivery-workers.py", ["--runtime", target / "runtime"]),
                       ("delivery-service.py", ["active", "--runtime", target / "runtime"])):
        run(sys.executable, "-B", ROOT / "scripts" / name, *args,
            stdout=subprocess.DEVNULL)


def provision(target):
    # Copy installed dependencies; an evicted package-manager cache must not be
    # part of the runtime's ownership chain. uv's base Python is checked below.
    env = {key: value for key, value in os.environ.items()
           if key not in {"VIRTUAL_ENV", "PYTHONPATH", "PYTHONHOME", "UV_PROJECT_ENVIRONMENT"}}
    env.update(UV_LINK_MODE="copy", PYTHONDONTWRITEBYTECODE="1")
    paths = load("installation-paths")
    base = paths.durable(Path(sys._base_executable).resolve(strict=True), "installation base Python")
    if not (target / "runtime/.venv").exists():
        # A stdlib environment has no additional virtualenv .pth hook. This
        # preserves the existing permanent-runtime migration's exact one-pth
        # contract instead of weakening that installed proof.
        run(base, "-I", "-B", "-m", "venv", "--without-pip", target / "runtime/.venv", env=env)
    run("uv", "sync", "--frozen", "--no-dev", "--python", base,
        cwd=target / "runtime", env=env)


def verify_console(paths, target):
    for name, module in paths.CONTROLLERS.items():
        entry = paths.published_entry(target, name)
        folder = entry.parent.parent
        metadata = paths.read_json(folder / "manifest.json")
        environment = {**os.environ, "DEVFLOW_INSTALLATION_PROBE_FD": str(_OWNER_FD)}
        try:
            result = run(metadata["base_python"], "-I", "-S", "-B", folder / "production-entry.py",
                         target, module, "--help", env=environment, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, text=True, timeout=45)
            if "usage:" not in result.stdout.lower():
                raise ValueError("production controller returned no actual help evidence: " + name)
        except subprocess.CalledProcessError as exc:
            raise ValueError(f"production controller readiness failed for {name}: {exc.stderr}") from exc


def rollback(journal, state, target):
    if state.get("backup"):
        rollback_controller(state, target).restore(Path(state["backup"]))
        state.pop("backup")
    if state.get("created"):
        if not state.get("target_identity") and state.get("stage") and os.path.lexists(state["stage"]):
            state.setdefault("unknown_allocations", []).append(state["stage"])
        owned = None
        for candidate in (target, Path(state["stage"]) if state.get("stage") else None):
            if candidate is None or not os.path.lexists(candidate):
                continue
            try:
                matches = state.get("target_identity") is not None and same_node(state["target_identity"], node(candidate))
            except (ValueError, OSError):
                matches = False  # An uninspectable foreign name is never ours.
            if matches:
                owned = candidate
                break
            state.setdefault("preserved_unattributed", []).append(str(candidate))
        if owned is None and state.get("target_identity"):
            raise ValueError("production rollback owner changed; retained journal requires reconciliation")
        retained = target.with_name("developer-workflow.unfinished-" + state["revision"][:12])
        if owned is not None and os.path.lexists(retained):
            raise ValueError("rollback retention destination exists; preserve it before retrying")
        if owned is not None:
            rename_exclusive(owned, retained)
            state["retained"] = str(retained)
    state["phase"] = "rolled_back"
    save(journal, state)
    print(json.dumps(state, sort_keys=True))


def install(arguments):
    global _OWNER_FD
    paths = load("installation-paths")
    clean = paths.git_environment()
    for key in tuple(os.environ):
        if key.startswith("GIT_") and key not in clean:
            del os.environ[key]
    force = bool(arguments and arguments[0] == "--force")
    if force:
        arguments = arguments[1:]
    reversing = arguments == ["--rollback"]
    if len(arguments) > 2:
        raise ValueError("usage: install.sh [--force] [skills-dir] [codex-home] | --rollback")
    owner = paths.durable(Path.home() / ".local/share/devflow")
    target = owner / "developer-workflow"
    if not reversing:
        paths.durable(target)
    paths.protected_directory(owner, create=True)
    lock = owner / ".production-install.lock"
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    original_umask = os.umask(0o077)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _OWNER_FD = fd
        paths._CONTROLLER_FD = fd
        journal = owner / ".production-install.json"
        state = json.loads(journal.read_text()) if journal.exists() else None
        if reversing:
            if not state or state["phase"] in {"verified", "rolled_back"}:
                raise ValueError("there is no unfinished production installation to roll back")
            rollback(journal, state, target)
            return
        skills = Path(arguments[0] if arguments else Path.home() / ".agents/skills").absolute()
        home = Path(arguments[1] if len(arguments) > 1 else
                    os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).absolute()
        paths.durable(skills, "production skill registration")
        paths.durable(home, "production agent registration")
        # Capture the original checkout identity before any clone/setup effects.
        revision = paths.git(ROOT, "rev-parse", "HEAD")
        if paths.git(ROOT, "status", "--porcelain", "--untracked-files=normal"):
            raise ValueError("commit or preserve setup checkout changes before production installation")
        if state and state["phase"] not in {"verified", "rolled_back"}:
            if (state["revision"] != revision or state["skills"] != str(skills)
                    or state["home"] != str(home)):
                raise ValueError("unfinished production installation: replay the same request or use --rollback")
            if state.get("created") and state["phase"] in {"requested", "allocating"}:
                publish_created(journal, state, target)
            if not target.exists() or not same_node(state.get("target_identity"), node(target)):
                raise ValueError("unfinished production source identity changed; retain it for reconciliation")
            if state.get("backup"):
                rollback_controller(state, target).restore(Path(state["backup"]))
                state.pop("backup")
                save(journal, state)
        else:
            prior = state
            state = {"schema_version": 1, "source": str(ROOT), "revision": revision,
                     "skills": str(skills), "home": str(home), "phase": "requested",
                     "created": not target.exists()}
            state["managed_owner_version"] = (1 if state["created"] else
                prior.get("managed_owner_version", 0) if prior and
                same_node(prior.get("target_identity"), node(target)) else 0)
            if not state["created"] and state["managed_owner_version"] != 1:
                raise ValueError("legacy production owner requires an authenticated complete stopped cutover")
            state["owner_generation"] = (uuid.uuid4().hex if state["created"] else prior.get("owner_generation"))
            state["born_new_owner"] = state["created"] or bool(prior.get("born_new_owner") if prior else False)
            if target.exists():
                paths.read_inventory(target, prior)
                identity = paths.source(target)
                if identity["revision"] != revision:
                    raise ValueError("durable runtime is a different revision; use its stopped update/installation transition")
                state["target_identity"] = node(target)
                # Reuse an already independently verified environment. Never
                # replace an existing environment merely because setup was
                # requested from another checkout.
                paths.runtime(target, allow_unpublished=True)
                state["reuse_environment"] = True
            save(journal, state)
            if state["created"]:
                publish_created(journal, state, target)
        if not (target / ".git").exists():
            if any(target.iterdir()):
                raise ValueError("incomplete clone has unrecognized contents; use --rollback to retain it")
            run("git", "clone", "--no-local", "--no-hardlinks", "--no-checkout", ROOT, target)
        if state["phase"] == "cloning":
            origin = paths.git(ROOT, "remote", "get-url", "origin")
            run("git", "-C", target, "remote", "set-url", "origin", origin)
            run("git", "-C", target, "checkout", "--detach", revision)
        paths.source(target)
        if not state.get("reuse_environment"):
            stopped(target)
            # Never resync an acknowledged installation in place: its measured
            # environment/source belong to a separate, durable authority transition.
            if not state["created"]:
                paths.require_update_inventory(target)
            state["phase"] = "provisioning"
            save(journal, state)
            provision(target)
        proof = paths.runtime(target, allow_unpublished=True)
        paths.initialize_inventory(target, state)
        paths.production_bootstrap(target, proof["base_python"], create=True)
        proof = paths.runtime(target)
        verify_console(paths, target)
        if not state.get("reuse_environment"):
            paths.sync_tree(target)
        state["proof"] = proof
        state["phase"] = "ready"
        save(journal, state)
        recovery = load("install-rollback", target)
        # The old source is deliberately retained for historical exec-updater
        # rollback authentication; installed links are created from target.
        import contextlib
        import io
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            recovery.capture(ROOT, skills, home)
        backup = Path(output.getvalue().strip())
        state["backup"] = str(backup)
        state["phase"] = "registering"
        save(journal, state)
        entry = load("install-service-entry", target)
        try:
            entry.install(skills, home, force, backup, migration_source=ROOT, lease_fd=fd)
            paths.runtime(target)
        except BaseException:
            if entry.MUTATING:
                recovery.restore(backup)
            else:
                recovery.restore(backup, checkout_only=True)
            state.pop("backup")
            state["phase"] = "ready"
            # Rollback may restore ROOT to a revision without these controller helpers.
            paths.write_json(journal, state)
            raise
        # Commit the durable outcome before discarding its recovery evidence.
        state["phase"] = "verified"
        save(journal, state)
        recovery.discard(backup)
        state.pop("backup")
        save(journal, state)
        print("Verified independent production runtime: " + str(target))
        print("Production command: " + str(paths.published_entry(target, "devflow-delivery")))
        print("Production MCP command: " + str(paths.published_entry(target, "devflow-delivery-mcp")))
    finally:
        _OWNER_FD = None
        paths._CONTROLLER_FD = None
        os.umask(original_umask)
        os.close(fd)


if __name__ == "__main__":
    try:
        install(sys.argv[1:])
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as exc:
        print("production installation: " + str(exc), file=sys.stderr)
        raise SystemExit(1) from exc
