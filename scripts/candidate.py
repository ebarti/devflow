#!/usr/bin/env python3.12
"""Run a candidate with separate local skills, hooks, sessions and metrics."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("trial_directory", type=Path)
    parser.add_argument("codex_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    source = Path(__file__).resolve().parent.parent
    trial = args.trial_directory.expanduser().resolve()
    codex_directory = trial / "codex"
    metadata = trial / "candidate.json"
    if (codex_directory / "config.toml").exists():
        previous = json.loads(metadata.read_text()) if metadata.exists() else {}
        if previous.get("source") != str(source):
            parser.error("trial configuration belongs to another source; use a new trial directory")
    codex_directory.mkdir(parents=True, exist_ok=True)
    skills = sorted(path.name for path in (source / "skills").iterdir() if path.is_dir())
    # Older installations may still expose skills this checkout no longer ships; a trial must not
    # load those either, so keep their names here after removing them from skills/.
    legacy_skills = ["using-devflow"]
    normal_roots = {Path.home() / ".agents/skills", Path.home() / ".codex/skills",
                    Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "skills"}
    config = ["[features]\nhooks = true\n"]
    for root in sorted(normal_roots):
        for name in [*skills, *legacy_skills]:
            path = root / name / "SKILL.md"
            if path.exists():
                config.append("[[skills.config]]\npath = " + json.dumps(str(path)) + "\nenabled = false\n")
    commit = subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output(["git", "-C", str(source), "status", "--porcelain"]))
    metadata.write_text(json.dumps({"source": str(source), "commit": commit, "dirty": dirty}, indent=2) + "\n")
    (codex_directory / "config.toml").write_text("\n".join(config))
    env = dict(os.environ, CODEX_HOME=str(codex_directory), XDG_STATE_HOME=str(trial / "state"))
    env.setdefault("DEVFLOW_PYTHON", sys.executable)
    subprocess.run(["sh", str(source / "scripts/install.sh"), str(codex_directory / "skills"),
                    str(codex_directory)], env=env, check=True)
    if not args.prepare_only:
        os.execvpe("codex", ["codex", *args.codex_args], env)


if __name__ == "__main__":
    main()
