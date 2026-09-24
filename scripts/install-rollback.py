#!/usr/bin/env python3.12
"""Restore only installer-owned files if a service upgrade fails before activation."""

import base64
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile


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
    paths.update(path for path in agents.glob("devflow-*.toml") if path.is_symlink()
                 and path.resolve(strict=False).parent == source / "agents")
    paths.update(skills / path.name for path in (source / "skills").iterdir() if path.is_dir())
    if skills.is_dir():
        paths.update(path for path in skills.iterdir() if path.is_symlink()
                     and path.resolve(strict=False).parent == source / "skills")
    return sorted(paths)


def capture(source, skills, codex):
    directory = Path(tempfile.mkdtemp(prefix="devflow-install-rollback-"))
    directory.chmod(0o700)
    entries = {}
    for path in target_paths(source, skills, codex):
        if path.is_symlink():
            entries[str(path)] = {"type": "link", "target": os.readlink(path)}
        elif path.is_file():
            entries[str(path)] = {"type": "file", "bytes": base64.b64encode(path.read_bytes()).decode(),
                                  "mode": stat.S_IMODE(path.stat().st_mode)}
        else:
            entries[str(path)] = {"type": "absent"}
    (directory / "snapshot.json").write_text(json.dumps(entries, sort_keys=True))
    (directory / "snapshot.json").chmod(0o600)
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
    (directory / "prior-checkout.json").write_text(json.dumps(previous))
    print(directory)


def restore(directory):
    entries = json.loads((directory / "snapshot.json").read_text())
    for name, saved in entries.items():
        path = Path(name)
        if path.is_dir() and not path.is_symlink():
            raise ValueError("installer destination became a directory: " + name)
        path.unlink(missing_ok=True)
        if saved["type"] == "link":
            path.parent.mkdir(parents=True, exist_ok=True)
            path.symlink_to(saved["target"])
        elif saved["type"] == "file":
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(saved["bytes"]))
            path.chmod(saved["mode"])
    previous = json.loads((directory / "prior-checkout.json").read_text())
    if previous:
        source = Path(previous["source"])
        if subprocess.run(["git", "-C", str(source), "diff", "--quiet", "HEAD", "--"]).returncode:
            raise ValueError("checkout changed during failed install; restore previous commit manually")
        subprocess.run(["git", "-C", str(source), "checkout", "--detach", previous["head"]], check=True)
    # Run cleanup in this process: checkout may have replaced this helper on
    # disk, so the invoking shell cannot safely launch it again.
    discard(directory)


def discard(directory):
    if directory.name.startswith("devflow-install-rollback-") and (directory / "snapshot.json").is_file():
        shutil.rmtree(directory)
    else:
        raise ValueError("not a Devflow rollback directory: " + str(directory))


def main():
    if len(sys.argv) == 5 and sys.argv[1] == "capture":
        capture(*(Path(value).resolve() for value in sys.argv[2:]))
    elif len(sys.argv) == 3 and sys.argv[1] == "restore":
        restore(Path(sys.argv[2]))
    elif len(sys.argv) == 3 and sys.argv[1] == "discard":
        discard(Path(sys.argv[2]))
    else:
        raise ValueError("usage: install-rollback.py capture SOURCE SKILLS CODEX | restore BACKUP | discard BACKUP")


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1)
