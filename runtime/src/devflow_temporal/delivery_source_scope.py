"""Source execution authority, separate from a plan's expected edit paths.

A legacy allowed_paths list continues to authorize exact files only. Explicit
source_scope v1 authorizes files under directory roots plus exact files, with
protected file/directory prefixes taking precedence. Merely adopting a v2 plan
never changes this frozen policy.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath

_CONTROL_PARTS = frozenset({".git", ".codex"})
_CONTROL_FILES = frozenset({".gitattributes", ".gitmodules"})


def source_path(raw: str, *, root: bool = False, protected: bool = False) -> str:
    if (
        not isinstance(raw, str)
        or not raw
        or len(raw) > 4096
        or "\x00" in raw
        or "\\" in raw
        or raw.startswith("-")
        or any(ord(char) < 32 for char in raw)
    ):
        raise ValueError("source authority requires a fixed relative path")
    if root and raw == ".":
        return raw
    path = PurePosixPath(raw)
    if (
        path.is_absolute()
        or raw != path.as_posix()
        or not path.parts
        or any(part in {".", ".."} for part in path.parts)
        or not protected
        and (any(part in _CONTROL_PARTS for part in path.parts) or path.name in _CONTROL_FILES)
    ):
        raise ValueError("source path controls Git or Codex configuration")
    return raw


def validate_authority(policy: dict) -> dict:
    """Validate explicit policy; legacy lists retain their exact-file meaning."""
    scope = policy.get("source_scope")
    legacy = policy.get("allowed_paths", [])
    if not isinstance(legacy, list) or any(not isinstance(p, str) for p in legacy):
        raise ValueError("allowed paths must be an exact relative file list")
    if scope is None:
        for raw in legacy:
            path = Path(raw)
            if (
                path.is_absolute()
                or not path.parts
                or raw != path.as_posix()
                or any(part in {".", "..", *_CONTROL_PARTS} for part in path.parts)
                or path.name in _CONTROL_FILES
            ):
                raise ValueError("allowed feature path controls Git or Codex configuration")
        return {
            "version": 0,
            "allowed_roots": [],
            "allowed_files": list(legacy),
            "protected_paths": [],
        }
    if (
        not isinstance(scope, dict)
        or set(scope) != {"version", "allowed_roots", "allowed_files", "protected_paths"}
        or type(scope["version"]) is not int
        or scope["version"] != 1
    ):
        raise ValueError("unsupported versioned source scope")
    if legacy:
        raise ValueError("versioned source scope cannot replace a nonempty legacy allowlist")
    for field in ("allowed_roots", "allowed_files", "protected_paths"):
        values = scope[field]
        if (
            not isinstance(values, list)
            or len(values) > 256
            or any(not isinstance(p, str) for p in values)
            or len(values) != len(set(values))
        ):
            raise ValueError("source scope paths must be bounded and unique")
        for raw in values:
            source_path(raw, root=field == "allowed_roots", protected=field == "protected_paths")
            if field != "protected_paths" and ".agents" in PurePosixPath(raw).parts:
                raise ValueError("source scope controls agent configuration")
    if not scope["allowed_roots"] and not scope["allowed_files"]:
        raise ValueError("versioned source scope must authorize source")
    return {key: list(value) if isinstance(value, list) else value for key, value in scope.items()}


def authority(policy: dict) -> dict:
    return validate_authority(policy)


def _under(path: str, prefix: str) -> bool:
    return prefix == "." or path == prefix or path.startswith(prefix + "/")


def outside_scope(policy: dict, paths, *, checkout: Path | None = None) -> set[str]:
    scope = authority(policy)
    escaped = set()
    for raw in paths:
        if scope["version"] == 0:
            if raw not in scope["allowed_files"]:
                escaped.add(raw)
            continue
        try:
            source_path(raw)
        except ValueError:
            escaped.add(raw)
            continue
        if (
            ".agents" in PurePosixPath(raw).parts
            or any(_under(raw, p) for p in scope["protected_paths"])
            or not (
                raw in scope["allowed_files"] or any(_under(raw, p) for p in scope["allowed_roots"])
            )
        ):
            escaped.add(raw)
            continue
        if checkout is not None:
            # A root is source authority, never permission to follow an external
            # directory link. Also reject links on deletions' surviving parents.
            path = checkout / raw
            if path.is_symlink() or path.resolve() != path or not path.is_relative_to(checkout):
                escaped.add(raw)
    return escaped


def require_authorized(policy: dict, paths, *, checkout: Path | None = None) -> None:
    escaped = outside_scope(policy, paths, checkout=checkout)
    if escaped:
        raise ValueError("candidate changed outside allowed paths: " + ", ".join(sorted(escaped)))
