"""Versioned repository policy, kept separate from private host bindings."""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from dataclasses import dataclass
from pathlib import Path

from devflow.errors import WorkflowError


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


@dataclass(frozen=True)
class RepositoryProfile:
    root: Path
    repository: dict
    recipes: dict[str, dict]
    lock: dict
    fingerprint: str
    sources: tuple[dict, ...]

    def recipe(self, recipe_id: str) -> dict:
        try:
            return dict(self.recipes[recipe_id])
        except KeyError as exc:
            raise WorkflowError("unknown_recipe", f"No check recipe {recipe_id!r}") from exc


def _toml(path: Path, root: Path) -> tuple[dict, str]:
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
        raise WorkflowError("profile_missing", f"Expected a regular profile file: {path}")
    raw = path.read_bytes()
    try:
        value = tomllib.loads(raw.decode())
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise WorkflowError("profile_invalid", f"Invalid TOML in {path.name}") from exc
    if type(value.get("schema_version")) is not int or value["schema_version"] != 1:
        raise WorkflowError("profile_version", f"Unsupported schema in {path.name}")
    return value, hashlib.sha256(raw).hexdigest()


def load_profile(root: Path | str) -> RepositoryProfile:
    root = Path(root).resolve(strict=True)
    directory = root / ".devflow"
    if directory.is_symlink():
        raise WorkflowError("profile_invalid", "Repository profile directory cannot be a symlink")
    parsed, sources = {}, []
    for name in ("repository.toml", "checks.toml", "workflow.lock"):
        parsed[name], file_hash = _toml(directory / name, root)
        sources.append({"reference": f".devflow/{name}", "hash": file_hash})
    repository = parsed["repository.toml"]
    if not isinstance(repository.get("repository"), dict):
        raise WorkflowError("profile_invalid", "repository.toml requires a repository table")
    identity = repository["repository"]
    if not all(
        isinstance(identity.get(k), str) and identity[k].strip() for k in ("id", "default_branch")
    ):
        raise WorkflowError("profile_invalid", "Repository id and default_branch are required")
    lock = parsed["workflow.lock"]
    if not re.fullmatch(r"[0-9a-f]{40}", str(lock.get("revision", ""))):
        raise WorkflowError("workflow_unpinned", "workflow.lock must pin a full Git commit SHA")
    if not isinstance(lock.get("version"), str) or not lock["version"].strip():
        raise WorkflowError("workflow_unpinned", "workflow.lock requires a release version")
    recipes = parsed["checks.toml"].get("checks")
    if not isinstance(recipes, dict) or not recipes:
        raise WorkflowError("profile_invalid", "checks.toml requires named checks")
    for name, recipe in recipes.items():
        _validate_recipe(name, recipe)
    return RepositoryProfile(root, repository, recipes, lock, digest(sources), tuple(sources))


def _validate_recipe(name: str, recipe: dict) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]*", name) or not isinstance(recipe, dict):
        raise WorkflowError("profile_invalid", "Invalid check name or definition")
    argv = recipe.get("argv")
    if (
        not isinstance(argv, list)
        or not argv
        or not all(isinstance(a, str) and a and "\0" not in a for a in argv)
    ):
        raise WorkflowError("profile_invalid", f"{name}: argv must be a nonempty string array")
    kind = recipe.get("kind")
    if kind not in {"static", "junit"}:
        raise WorkflowError("profile_invalid", f"{name}: expected static or junit result kind")
    if kind == "junit" and not any("{report_path}" in a for a in argv):
        raise WorkflowError("profile_invalid", f"{name}: JUnit command must use {{report_path}}")
    cwd = Path(recipe.get("cwd", "."))
    if cwd.is_absolute() or ".." in cwd.parts:
        raise WorkflowError("profile_invalid", f"{name}: cwd must stay within the repository")
    timeout = recipe.get("timeout_seconds", 300)
    if type(timeout) is not int or not 1 <= timeout <= 7200:
        raise WorkflowError("profile_invalid", f"{name}: invalid timeout_seconds")
    for key, default in (("min_executed", 1), ("max_skipped", 0)):
        value = recipe.get(key, default)
        if type(value) is not int or value < (1 if key == "min_executed" else 0):
            raise WorkflowError("profile_invalid", f"{name}: invalid {key}")
    if not isinstance(recipe.get("description"), str) or not recipe["description"].strip():
        raise WorkflowError("profile_invalid", f"{name}: describe the invariant being checked")
    scenarios = recipe.get("scenarios", [])
    if not isinstance(scenarios, list) or not all(isinstance(s, str) and s for s in scenarios):
        raise WorkflowError("profile_invalid", f"{name}: scenarios must be a string array")


def assert_admitted_profile(profile: RepositoryProfile, reference: str) -> None:
    if reference != f"sha256:{profile.fingerprint}":
        raise WorkflowError(
            "profile_drift", "Repository policy changed after admission; reconcile before execution"
        )
