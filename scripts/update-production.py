#!/usr/bin/env python3.12
"""Update the stopped durable owner; retain source/environment rollback evidence."""

from __future__ import annotations

import fcntl
import importlib.util
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONTROL = ("install-production.py", "update-production.py", "installation-paths.py",
           "production-entry.py", "install-rollback.py", "delivery-workers.py", "delivery-service.py")


def load(name, root=ROOT):
    spec = importlib.util.spec_from_file_location(name, root / "scripts" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def recovery_root(paths, target, state, *, create=False):
    root = target.parent / ".production-recovery" / state["before"]
    paths.protected_directory(root / "scripts", create=create)
    for name in CONTROL:
        wanted = paths.git(target, "show", state["before"] + ":scripts/" + name) + "\n"
        file = root / "scripts" / name
        if not file.exists() and create:
            fd = os.open(file, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(wanted)
                stream.flush()
                os.fsync(stream.fileno())
        if file.is_symlink() or file.read_text() != wanted:
            raise ValueError("retained recovery controller differs from its previous Git object")
    return root


def snapshot_environment(install, paths, target, journal, state):
    parent = target.parent / ".production-environments"
    paths.protected_directory(parent, create=True)
    container = Path(tempfile.mkdtemp(prefix="previous-", dir=parent))
    snapshot = container / "venv"
    state.update(environment_snapshot=str(snapshot), environment_before=install.node(target / "runtime/.venv"),
                 phase="snapshotting")
    install.save(journal, state)
    shutil.copytree(target / "runtime/.venv", snapshot, symlinks=True)
    directories = [container, snapshot]
    for item in snapshot.rglob("*"):
        if item.is_symlink():
            continue
        if item.is_dir():
            directories.append(item)
        else:
            fd = os.open(item, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
    for directory in reversed(directories):
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    proof = paths.runtime(target, environment=snapshot)
    state["recovery_python"] = proof["base_python"]
    state["environment_snapshot_identity"] = install.node(snapshot)
    state["environment_snapshot_ready"] = True
    install.save(journal, state)


def restore_environment(install, target, state, recovery):
    env = target / "runtime/.venv"
    if not state.get("environment_snapshot_ready"):
        if not install.same_node(state.get("environment_before", install.node(env)), install.node(env)):
            raise ValueError("environment changed before a complete offline snapshot; preserve recovery evidence")
        return
    snapshot = Path(state["environment_snapshot"])
    expected = state["environment_snapshot_identity"]
    if os.path.lexists(env) and install.same_node(expected, install.node(env)):
        return  # The exchange committed before a response/journal write.
    if not os.path.lexists(snapshot) or not install.same_node(expected, install.node(snapshot)):
        raise ValueError("offline environment snapshot changed or vanished")
    if os.path.lexists(env):
        recovery.exchange(snapshot, env)
    else:
        install.rename_exclusive(snapshot, env)
    state["retained_failed_environment"] = str(snapshot)


def recover(install, paths, target, journal, state):
    recovery = load("install-rollback", recovery_root(paths, target, state))
    if state.get("backup"):
        recovery.restore(Path(state["backup"]))
        state.pop("backup")
        install.save(journal, state)
    if paths.git(target, "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("update recovery found changed source; retain the journal and preserve those edits")
    install.run("git", "-C", target, "checkout", "--detach", state["before"])
    restore_environment(install, target, state, recovery)
    paths.runtime(target)
    install.verify_console(paths, target)
    if state["branch"]:
        install.run("git", "-C", target, "checkout", state["branch"])
    if state.get("before_install_receipt"):
        install.save(target.parent / ".production-install.json", state["before_install_receipt"])
    state["phase"] = "rolled_back"
    install.save(journal, state)


def update(arguments):
    if not 1 <= len(arguments) <= 3:
        raise ValueError("usage: update.sh TAG [skills-directory] [codex-home] | --rollback")
    paths = load("installation-paths")
    clean = paths.git_environment()
    for key in tuple(os.environ):
        if key.startswith("GIT_") and key not in clean:
            del os.environ[key]
    install = load("install-production")
    target = paths.durable(Path.home() / ".local/share/devflow/developer-workflow")
    paths.protected_directory(target)
    lock = os.open(target.parent / ".production-install.lock",
                   os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    original_umask = os.umask(0o077)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        install._OWNER_FD = lock
        paths._CONTROLLER_FD = lock
        journal = target.parent / ".production-update.json"
        state = json.loads(journal.read_text()) if journal.exists() else None
        install.stopped(target)
        paths.require_update_inventory(target)
        if arguments == ["--rollback"]:
            if not state or state["phase"] in {"verified", "rolled_back"}:
                raise ValueError("there is no unfinished production update")
            recover(install, paths, target, journal, state)
            return
        tag = arguments[0]
        install.run("git", "check-ref-format", "refs/tags/" + tag)
        skills = Path(arguments[1] if len(arguments) > 1 else Path.home() / ".agents/skills").absolute()
        home = Path(arguments[2] if len(arguments) > 2 else
                    os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))).absolute()
        paths.durable(skills)
        paths.durable(home)
        if state and state["phase"] not in {"verified", "rolled_back"}:
            if (state["tag"] != tag or state["skills"] != str(skills)
                    or state["home"] != str(home)):
                raise ValueError("unfinished production update: replay the same request or use --rollback")
            # A lost response is not evidence that setup completed. Restore the
            # exact previous owner before retrying the requested release.
            recover(install, paths, target, journal, state)
        paths.runtime(target, allow_unpublished=True)
        state = {"schema_version": 1, "tag": tag, "skills": str(skills), "home": str(home),
                 "before": paths.git(target, "rev-parse", "HEAD"), "phase": "requested",
                 "before_install_receipt": paths.read_json(target.parent / ".production-install.json"),
                 "branch": paths.git(target, "symbolic-ref", "-q", "--short", "HEAD")
                 if os.path.exists(target / ".git/HEAD") and
                 (target / ".git/HEAD").read_text().startswith("ref: ") else ""}
        # The recovery controller must survive replacement or breakage of the
        # release's own scripts. It is authenticated against the previous Git
        # objects, and remains callable after a crash without the setup checkout.
        controller = recovery_root(paths, target, state, create=True)
        install.save(journal, state)
        try:
            install.run("git", "-C", target, "fetch", "--no-tags", "origin",
                        "refs/tags/" + tag + ":refs/tags/" + tag)
            state["candidate"] = paths.git(target, "rev-parse", "--verify", "refs/tags/" + tag + "^{commit}")
            for name in (*CONTROL, "install-service-entry.py", "install-agents.py", "install-migration.py"):
                paths.git(target, "cat-file", "-e", state["candidate"] + ":scripts/" + name)
            paths.git(target, "cat-file", "-e", state["candidate"] + ":runtime/uv.lock")
            paths.git(target, "cat-file", "-e", state["candidate"] +
                      ":runtime/src/devflow_temporal/delivery_installation_owner.py")
            paths.git(target, "cat-file", "-e", state["candidate"] + ":scripts/production-entry.py")
            snapshot_environment(install, paths, target, journal, state)
            # This interpreter survives setup checkout removal and candidate
            # dependency replacement. Its stdlib/base interpreter were proved
            # together with the independent retained environment.
            state["recovery_command"] = [state["recovery_python"], "-I", "-S", "-B",
                                         str(controller / "scripts/update-production.py"), "--rollback"]
            install.save(journal, state)
            # Snapshot registration objects before changing source or environment.
            import contextlib
            import io
            output = io.StringIO()
            recovery = load("install-rollback", target)
            with contextlib.redirect_stdout(output):
                recovery.capture(target, skills, home)
            backup = Path(output.getvalue().strip())
            state["backup"] = str(backup)
            state["phase"] = "updating"
            install.save(journal, state)
            install.run("git", "-C", target, "checkout", "--detach", state["candidate"])
            install.provision(target)
            proof = paths.runtime(target, allow_unpublished=True)
            paths.production_bootstrap(target, proof["base_python"], create=True)
            install.verify_console(paths, target)
            paths.sync_tree(target)
            state["phase"] = "registering"
            install.save(journal, state)
            load("install-service-entry", target).install(skills, home, False, backup,
                                                         migration_source=target, lease_fd=lock)
            state["proof"] = paths.runtime(target)
            # Publish observations while the update still blocks all starts.
            # A crash here remains recoverable using the original receipt;
            # terminal verification must never expose the previous revision.
            receipt = target.parent / ".production-install.json"
            installed = dict(state["before_install_receipt"])
            installed.update(revision=state["candidate"], proof=state["proof"], phase="verified")
            install.save(receipt, installed)
            state["phase"] = "verified"
            install.save(journal, state)
            recovery.discard(backup)
            state.pop("backup")
            install.save(journal, state)
            print("Verified production update: " + state["candidate"])
        except BaseException:
            # Preserve the primary failure. Any failed recovery remains an
            # unfinished journal; its public rollback can be replayed.
            if state["phase"] != "verified":
                recover(install, paths, target, journal, state)
            raise
    finally:
        install._OWNER_FD = None
        paths._CONTROLLER_FD = None
        os.umask(original_umask)
        os.close(lock)


if __name__ == "__main__":
    try:
        update(sys.argv[1:])
    except Exception as exc:
        print("production update: " + str(exc), file=sys.stderr)
        raise SystemExit(1) from exc
