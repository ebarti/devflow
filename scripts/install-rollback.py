#!/usr/bin/env python3.12
"""Restore only installer-owned files if a service upgrade fails before activation."""

import base64
import ctypes
import hashlib
import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path


def exchange_function():
    """Require the native no-gap exchange; unsupported hosts never use replace."""
    library = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        function = getattr(library, "renamex_np", None)
        arguments = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint]
    elif sys.platform.startswith("linux"):
        function = getattr(library, "renameat2", None)
        arguments = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    else:
        function = None
    if function is None:
        raise ValueError("atomic installer rollback requires native Linux/macOS exchange")
    function.argtypes, function.restype = arguments, ctypes.c_int
    return function


def exchange(left, right):
    function = exchange_function()
    args = [os.fsencode(left), os.fsencode(right), 2]  # RENAME_SWAP / RENAME_EXCHANGE
    if sys.platform.startswith("linux"):
        args = [-100, args[0], -100, args[1], args[2]]  # AT_FDCWD
    if function(*args):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(right))


def identity(path, *, saved=None):
    if not os.path.lexists(path):
        return {"type": "absent"}
    info = path.lstat()
    result = {"node": [info.st_dev, info.st_ino, info.st_uid, info.st_mode]}
    if path.is_symlink():
        result.update(type="link", target=os.readlink(path))
    elif stat.S_ISREG(info.st_mode):
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as stream:
            fields = ("st_dev", "st_ino", "st_uid", "st_mode", "st_nlink",
                      "st_size", "st_mtime_ns", "st_ctime_ns")
            observation = tuple(getattr(info, field) for field in fields)
            opened = os.fstat(stream.fileno())
            if tuple(getattr(opened, field) for field in fields) != observation:
                raise ValueError("installer path changed while reading: " + str(path))
            info = opened
            raw = stream.read()
            after = os.fstat(stream.fileno())
            if tuple(getattr(after, field) for field in fields) != observation:
                raise ValueError("installer file changed while reading: " + str(path))
            result.update(type="file", sha256=hashlib.sha256(raw).hexdigest(),
                          version=[info.st_nlink, info.st_size, info.st_mtime_ns])
            if saved is not None:
                saved.update(bytes=base64.b64encode(raw).decode(), mode=stat.S_IMODE(info.st_mode))
    elif stat.S_ISDIR(info.st_mode):
        result["type"] = "directory"
    else:
        raise ValueError("unsupported installer object preserved: " + str(path))
    return result


def canonical_target(path):
    # Resolve ancestors, never the registered object itself (often a symlink).
    return Path(path).parent.resolve() / Path(path).name


def preflight(directory, paths):
    """Check the actual backup/target boundaries before installation effects."""
    parents = set()
    for path in paths:
        parent = path.parent
        while not parent.exists():
            parent = parent.parent
        if not parent.is_dir() or parent.stat().st_dev != directory.stat().st_dev:
            raise ValueError(f"atomic installation requires one filesystem: {path}; "
                             f"backup {directory}; choose skills and Codex locations on the same volume")
        parents.add(parent)
    for parent in parents:
        with tempfile.TemporaryDirectory(prefix=".devflow-exchange-check-", dir=parent) as probe:
            stage, target = directory / ".exchange-check", Path(probe) / "target"
            try:
                stage.write_bytes(b"backup")
                target.write_bytes(b"target")
                exchange(stage, target)
            except OSError as exc:
                raise ValueError(f"atomic exchange unsupported at {parent}: {exc}; "
                                 "use locations supporting native exchange before retrying") from exc
            finally:
                stage.unlink(missing_ok=True)


def remember(directory, path, installed):
    """Bind one actual installer effect inside its existing private snapshot."""
    if directory is None:
        return
    path = canonical_target(path)
    snapshot = Path(directory) / "snapshot.json"
    entries = json.loads(snapshot.read_text())
    saved = entries.get(str(path))
    if saved is None:
        raise ValueError("installer effect is outside captured targets: " + str(path))
    if identity(path) != saved.get("installed", saved["before"]):
        raise ValueError("post-capture data preserved before installer effect: " + str(path))
    saved["installed"] = installed
    save_snapshot(snapshot, entries)


def effect(directory, path, stage=None):
    """Authenticate the object displaced by a forward installer mutation."""
    path = canonical_target(path)
    snapshot = Path(directory) / "snapshot.json"
    entries = json.loads(snapshot.read_text())
    expected = entries[str(path)].get("installed", entries[str(path)]["before"])
    remember(directory, path, identity(stage) if stage else {"type": "absent"})
    if expected["type"] == "absent":
        if stage:
            os.link(stage, path, follow_symlinks=False)  # Never overwrite a new object.
        return
    captured = Path(directory) / ("forward-" + hashlib.sha256(str(path).encode()).hexdigest())
    if stage:
        os.replace(stage, captured)  # Cross-device failure precedes public mutation.
        exchange(captured, path)  # The displaced object is already in the private backup.
    else:
        os.replace(path, captured)
    reason = None
    try:
        matches = identity(captured) == expected
    except (OSError, ValueError) as exc:
        matches, reason = False, str(exc)
    if not matches:
        message = f"{path} captured at {captured}"
        entries = json.loads(snapshot.read_text())
        entries[str(path)]["drift"] = message + (f"; authentication failed: {reason}" if reason else "")
        info = captured.lstat()
        entries[str(path)]["displaced_node"] = [info.st_dev, info.st_ino, info.st_uid, info.st_mode]
        save_snapshot(snapshot, entries)
        if stage is None:
            try:
                os.link(captured, path, follow_symlinks=False)
            except OSError:
                pass  # Never overwrite a concurrently recreated destination.
        raise ValueError("forward installer drift preserved: " + message
                         + "; inspect retained backup " + str(directory))
    captured.unlink()  # Only the authenticated prior installer object is retired.


def save_snapshot(snapshot, entries):
    temporary = snapshot.with_name(".snapshot-" + str(os.getpid()))
    try:
        with temporary.open("x") as stream:
            stream.write(json.dumps(entries, sort_keys=True))
        temporary.chmod(0o600)
        os.replace(temporary, snapshot)
    finally:
        temporary.unlink(missing_ok=True)


def created(directory, path):
    # Creation itself is exclusive (mkdir/symlink). Record its actual inode;
    # no previous object was overwritten and subsequent drift will not match.
    if directory is None:
        return
    path = canonical_target(path)
    snapshot = Path(directory) / "snapshot.json"
    entries = json.loads(snapshot.read_text())
    saved = entries[str(path)]
    if saved["before"] != {"type": "absent"}:
        raise ValueError("created installer path was not absent: " + str(path))
    saved["installed"] = identity(path)
    save_snapshot(snapshot, entries)


def target_paths(source, skills, codex):
    paths = {codex / name for name in (".devflow-install.json", ".devflow-hook.py", "hooks.json")}
    agents = codex / "agents"
    paths.add(agents / ".devflow-agent-manifest.json")
    paths.update(agents / item.name for item in (source / "agents").glob("devflow-*.toml"))
    manifest = agents / ".devflow-agent-manifest.json"
    if manifest.is_file() and not manifest.is_symlink():
        try:
            saved = json.loads(manifest.read_text())
            paths.update(agents / name for name in saved["agents"]
                         if isinstance(name, str) and Path(name).name == name
                         and name.startswith("devflow-") and name.endswith(".toml"))
        except (OSError, ValueError, KeyError, TypeError):
            # Preflight will reject a malformed manifest; no file mutation has
            # happened, but checkout rollback must still be available.
            paths.update(agents.glob("devflow-*.toml"))
    paths.add(skills / "devflow-local-delivery")
    paths.update(path for path in agents.glob("devflow-*.toml") if path.is_symlink()
                 and path.resolve(strict=False).parent == source / "agents")
    paths.update(skills / path.name for path in (source / "skills").iterdir() if path.is_dir())
    if skills.is_dir():
        paths.update(path for path in skills.iterdir() if path.is_symlink()
                     and path.resolve(strict=False).parent == source / "skills")
    for directory in (skills / "devflow", skills / ".devflow-helpers"):
        paths.add(directory)
        if not directory.is_symlink() or directory == skills / "devflow":
            # A retained compatibility root may itself be a symlink. Capture
            # the actual child registrations before any authenticated exchange.
            paths.update(directory / item.name for item in (source / "skills/devflow").iterdir()
                         if item.name not in {"SKILL.md", "__pycache__"})
    return sorted({canonical_target(path) for path in paths},
                  key=lambda item: (-len(item.parts), str(item)))


def restore_checkout(previous):
    if previous:
        source = Path(previous["source"])
        if subprocess.run(["git", "-C", str(source), "diff", "--quiet", "HEAD", "--"]).returncode:
            raise ValueError("checkout changed during failed install; restore previous commit manually")
        subprocess.run(["git", "-C", str(source), "checkout", "--detach", previous["head"]], check=True)


def backup_directory(codex):
    anchor = codex if codex.exists() else codex.parent
    while True:
        while not anchor.exists():
            anchor = anchor.parent
        info = anchor.lstat()
        if (stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
                and info.st_mode & 0o022 == 0o020 and anchor.parent != anchor):
            anchor = anchor.parent  # The installer itself may create a mode-0775 Codex directory.
            continue
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise ValueError("backup parent must be owned and protected: " + str(anchor))
        break
    directory = Path(tempfile.mkdtemp(prefix="devflow-install-rollback-", dir=anchor))
    directory.chmod(0o700)
    return directory


def capture(source, skills, codex):
    # Pre-marker failures can occur when an older updater has already switched
    # the checkout. That updater may `exec` the new installer and never run
    # again, so retain its previous detached checkout for failure recovery.
    previous = None
    linked = skills / "devflow"
    pin_path = codex / ".devflow-install.json"
    pin_head = None
    pin_present = pin_path.exists() or pin_path.is_symlink()
    if pin_path.is_file() and not pin_path.is_symlink():
        try:
            pin = json.loads(pin_path.read_text())
            if pin.get("source") == str(source):
                pin_head = pin.get("head")
        except (OSError, ValueError, AttributeError):
            pass
    if linked.is_symlink() and linked.resolve(strict=False) == source / "skills/devflow":
        try:
            current = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
            prior = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD@{1}"], text=True).strip()
            last = subprocess.check_output(["git", "-C", str(source), "reflog", "-1", "--format=%gs"], text=True).strip()
            detached = subprocess.run(["git", "-C", str(source), "symbolic-ref", "-q", "HEAD"],
                                      capture_output=True).returncode != 0
            clean = subprocess.run(["git", "-C", str(source), "diff", "--quiet", "HEAD", "--"]).returncode == 0
            if (detached and clean and current != prior and last.startswith("checkout: moving from ")
                    and (not pin_present or pin_head == prior)):
                previous = {"source": str(source), "head": prior}
        except subprocess.CalledProcessError:
            pass
    try:
        directory = backup_directory(codex)
        (directory / "prior-checkout.json").write_text(json.dumps(previous))
        snapshot = directory / "snapshot.json"
        save_snapshot(snapshot, {})
    except (OSError, ValueError):
        restore_checkout(previous)  # No destination effect can precede snapshot preparation.
        raise
    try:
        paths = target_paths(source, skills, codex)
        preflight(directory, paths)
        entries = {}
        for path in paths:
            saved = {}
            before = identity(path, saved=saved)
            saved.update(type=before["type"], before=before)
            if before["type"] == "link":
                saved["target"] = before["target"]
            entries[str(path)] = saved
        save_snapshot(snapshot, entries)
    except (OSError, ValueError):
        # Capture has no destination effects. The exec-based historical updater
        # cannot recover its checkout after this process refuses the read.
        restore(directory, checkout_only=True)
        raise
    print(directory)


def restore_targets(directory, conflicts):
    entries = json.loads((directory / "snapshot.json").read_text())
    bound, helpers = {}, set()
    for name, saved in entries.items():
        if "installed" not in saved:
            continue  # This installer never mutated the captured path.
        try:
            if identity(Path(name)) == saved["installed"]:
                bound[name] = saved
                continue
            conflicts.append(name)
        except (OSError, ValueError) as exc:
            conflicts.append(f"{name}: destination recovery failed: {exc}")
        if Path(name).name == "devflow" and saved["type"] == "link":
            helpers.add(Path(name).parent / ".devflow-helpers")
    entries = bound
    # Exchange restores public pointers without a gap and captures their prior
    # inode privately. Cleanup similarly captures before authenticating/deleting.
    def restore_pointer(name, saved):
        if saved["type"] != "link":
            return
        path = Path(name)
        if path.is_symlink() and os.readlink(path) == saved["target"]:
            return
        stage = directory / ("restore-" + hashlib.sha256(name.encode()).hexdigest())
        stage.symlink_to(saved["target"])
        if saved["installed"]["type"] == "absent":
            try:
                os.link(stage, path, follow_symlinks=False)
            except FileExistsError:
                conflicts.append(name)
        else:
            exchange(stage, path)
            if identity(stage) != saved["installed"]:
                conflicts.append(f"{name} captured at {stage}")
                return
        stage.unlink()

    def restore_object(name, saved):
        path = Path(name)
        if saved["type"] == "link":
            return  # The public pointer has already been restored atomically.
        if (saved["type"] == "file" and path.is_file() and not path.is_symlink()
                and path.read_bytes() == base64.b64decode(saved["bytes"])
                and stat.S_IMODE(path.stat().st_mode) == saved["mode"]):
            return
        if saved["type"] == "file":
            stage = directory / ("restore-" + hashlib.sha256(name.encode()).hexdigest())
            with stage.open("xb") as stream:
                stream.write(base64.b64decode(saved["bytes"]))
            stage.chmod(saved["mode"])
            if saved["installed"]["type"] == "absent":
                try:
                    os.link(stage, path, follow_symlinks=False)
                except FileExistsError:
                    conflicts.append(name)
            else:
                exchange(stage, path)
                if identity(stage) != saved["installed"]:
                    conflicts.append(f"{name} captured at {stage}")
                    return
            stage.unlink()
        elif saved["type"] == "absent" and os.path.lexists(path):
            if path.is_dir() and not path.is_symlink() and any(path.iterdir()):
                conflicts.append(name)
                return
            captured = directory / ("captured-" + hashlib.sha256(name.encode()).hexdigest())
            os.replace(path, captured)
            if identity(captured) != saved["installed"]:
                try:
                    os.link(captured, path, follow_symlinks=False)
                except OSError:
                    pass  # A recreated path or directory is never overwritten.
                conflicts.append(f"{name} captured at {captured}")
                return
            try:
                captured.rmdir() if captured.is_dir() and not captured.is_symlink() else captured.unlink()
            except OSError:
                conflicts.append(f"{name} captured at {captured}")

    for name, saved in entries.items():
        count = len(conflicts)
        try:
            restore_pointer(name, saved)
        except (OSError, ValueError, KeyError) as exc:
            conflicts.append(f"{name}: destination recovery failed: {exc}")
        if len(conflicts) != count and Path(name).name == "devflow":
            helpers.add(Path(name).parent / ".devflow-helpers")
    for name in sorted(entries, key=lambda value: (-len(Path(value).parts), value)):
        if any(Path(name).is_relative_to(helper) for helper in helpers):
            conflicts.append(name + ": helper cleanup awaits pointer recovery")
            continue
        try:
            restore_object(name, entries[name])
        except (OSError, ValueError, KeyError) as exc:
            conflicts.append(f"{name}: destination recovery failed: {exc}")
    return conflicts


def restore(directory, *, checkout_only=False):
    entries = json.loads((directory / "snapshot.json").read_text())
    conflicts = [saved["drift"] for saved in entries.values() if "drift" in saved]
    if not checkout_only:
        try:
            restore_targets(directory, conflicts)
        except (OSError, ValueError, KeyError) as exc:
            conflicts.append("destination recovery failed: " + str(exc))
    previous = json.loads((directory / "prior-checkout.json").read_text())
    try:
        restore_checkout(previous)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        conflicts.append("source checkout recovery failed: " + str(exc))
    # Run cleanup in this process: checkout may have replaced this helper on
    # disk, so the invoking shell cannot safely launch it again.
    if conflicts:
        raise ValueError("rollback drift preserved: " + ", ".join(conflicts)
                         + "; inspect retained backup " + str(directory))
    discard(directory)


def discard(directory):
    if directory.name.startswith("devflow-install-rollback-") and (directory / "snapshot.json").is_file():
        shutil.rmtree(directory)
    else:
        raise ValueError("not a Devflow rollback directory: " + str(directory))


def main():
    if len(sys.argv) == 5 and sys.argv[1] == "capture":
        capture(*(Path(value).resolve() for value in sys.argv[2:]))
    elif len(sys.argv) == 3 and sys.argv[1] in {"restore", "restore-checkout"}:
        restore(Path(sys.argv[2]), checkout_only=sys.argv[1] == "restore-checkout")
    elif len(sys.argv) == 3 and sys.argv[1] == "discard":
        discard(Path(sys.argv[2]))
    else:
        raise ValueError("usage: install-rollback.py capture SOURCE SKILLS CODEX | restore BACKUP | discard BACKUP")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc
