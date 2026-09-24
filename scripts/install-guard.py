#!/usr/bin/env python3.12
"""Pin the installed hook to the checkout contents seen during installation."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


def files(source):
    return {str(path.relative_to(source)): hashlib.sha256(path.read_bytes()).hexdigest()
            for name in ("skills", "agents", "scripts")
            for path in sorted((source / name).rglob("*"))
            if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"}


def head(source, git):
    return subprocess.check_output([git, "-C", str(source), "rev-parse", "HEAD"], text=True).strip()


def snapshot(source, skills, codex):
    codex.mkdir(parents=True, exist_ok=True)
    git = shutil.which("git")
    payload = {"source": str(source), "skills": str(skills), "git": git,
               "head": head(source, git), "files": files(source)}
    temporary = codex / (".devflow-install.json.tmp-" + str(os.getpid()))
    temporary.write_text(json.dumps(payload, sort_keys=True) + "\n")
    temporary.chmod(0o600)
    temporary.replace(codex / ".devflow-install.json")
    shutil.copyfile(__file__, codex / ".devflow-hook.py")
    (codex / ".devflow-hook.py").chmod(0o600)


def check(codex):
    try:
        saved = json.loads((codex / ".devflow-install.json").read_text())
        source, skills = Path(saved["source"]), Path(saved["skills"])
        if head(source, saved["git"]) != saved["head"] or files(source) != saved["files"]:
            return "installed Devflow checkout changed; restore it or rerun scripts/install.sh"
        for name in (path.name for path in (source / "skills").iterdir() if path.is_dir()):
            if (skills / name).resolve() != (source / "skills" / name).resolve() or not (skills / name / "SKILL.md").is_file():
                return "installed Devflow skill link is missing or changed; rerun scripts/install.sh"
        for path in (source / "agents").glob("*.toml"):
            target = codex / "agents" / path.name
            if target.is_symlink() or not target.is_file() or target.read_bytes() != path.read_bytes():
                return "installed Devflow agent copy is missing or changed; restore a matching copy before reinstalling"
    except (OSError, KeyError, ValueError, subprocess.CalledProcessError):
        return "installed Devflow source cannot be verified; restore it or rerun scripts/install.sh"
    return None


def main():
    args = sys.argv[1:]
    if len(args) == 4 and args[0] == "snapshot":
        snapshot(*(Path(arg).resolve() for arg in args[1:]))
        return 0
    codex = Path(__file__).resolve().parent
    error = check(codex)
    if args == ["--check"]:
        print(json.dumps({"status": "drift" if error else "current", "reason": error}))
        return 1 if error else 0
    if args:
        raise ValueError("usage: install-guard.py [--check]")
    if error:
        print(json.dumps({"systemMessage": "Devflow installation drift: " + error}))
        return 0
    saved = json.loads((codex / ".devflow-install.json").read_text())
    script = Path(saved["source"]) / "skills/devflow/scripts/telemetry.py"
    os.execv(sys.executable, [sys.executable, "-B", str(script), "hook"])


if __name__ == "__main__":
    raise SystemExit(main())
