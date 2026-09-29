"""Deterministic identity for every trusted Python file copied into the runner image."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def payload_digest(package: Path, launcher: Path) -> str:
    package = package.resolve(strict=True)
    launcher = launcher.resolve(strict=True)
    files = sorted(package.rglob("*.py"))
    if not files or not launcher.is_file():
        raise ValueError("runner payload files are missing")
    manifest: dict[str, str] = {}
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise ValueError("runner payload contains a linked or missing source")
        relative = path.relative_to(package).as_posix()
        manifest["devflow_temporal/" + relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest["landlock_exec.py"] = hashlib.sha256(launcher.read_bytes()).hexdigest()
    canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: payload.py PACKAGE_DIR LANDLOCK_EXEC")
    print(payload_digest(Path(sys.argv[1]), Path(sys.argv[2])))
