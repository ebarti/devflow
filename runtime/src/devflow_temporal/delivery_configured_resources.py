"""Frozen check declarations for Python environments created before planning."""

from __future__ import annotations

from pathlib import Path

from .contracts import digest


def python_environment_authority(spec: dict, relative: str) -> str | None:
    """Bind custody to the exact configured recipes, independently of intake."""
    path = Path(relative)
    if (spec["policy"].get("host_sandbox") != "trusted-local"
            or path.is_absolute() or path.as_posix() != relative or ".." in path.parts
            or path.name != ".venv"
            or any(part in {".git", ".codex", ".agents"} for part in path.parts)):
        return None
    recipes = {
        digest(check)
        for stage in ("baseline_checks", "prepublish_checks", "checks")
        for check in spec["policy"].get(stage, [])
        if isinstance(check, dict) and isinstance(check.get("generated_directories"), list)
        and relative in check["generated_directories"]
    }
    return digest({"path": relative, "recipes": sorted(recipes)}) if recipes else None


def require_locked_project(root: Path, relative: str) -> None:
    """A declaration cannot adopt generated metadata or redirect its project."""
    from .delivery_broker import _git

    project = (root / relative).parent
    for name in ("pyproject.toml", "uv.lock"):
        path = project / name
        raw = path.relative_to(root).as_posix()
        if (not path.is_file() or path.is_symlink() or path.resolve(strict=True) != path
                or _git(root, "ls-files", "--", raw) != raw):
            raise ValueError("configured Python environment lacks fixed tracked project metadata")
