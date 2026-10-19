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


def implementation_prerequisites(spec: dict) -> list[dict]:
    """Reuse frozen pre-feature environment recipes before implementation probes."""
    baseline = spec["policy"].get("baseline_checks", [])
    if ("baseline_checks" not in spec["policy"] and spec.get("feature_worker")
            and "baseline_checks_version" not in spec):
        from .delivery_execution_config import load_execution_config

        # Workers omit the coordinator's baseline gate. Its already frozen
        # configuration still authenticates which recipes are prerequisites.
        # Read it without changing the sealed child policy or old reservations.
        config = load_execution_config(spec)
        repository = config.raw["repositories"][spec["repository_key"]]
        selected = repository.get("baseline_check_ids", [])
        baseline = [check for check in repository.get("prepublish_checks", [])
                    if check["id"] in selected]
    result = []
    for check in spec["policy"].get("prepublish_checks", []):
        generated = check.get("generated_directories", [])
        python = (
            check in baseline and check.get("kind") != "test"
            and isinstance(generated, list)
            and any(isinstance(name, str) and python_environment_authority(spec, name)
                    for name in generated)
        )
        if "/store" in check["argv"] or python:
            result.append(check)
    return result


def declared_generated_directories(spec: dict) -> set[str]:
    """Only operator-frozen outputs, excluding separately governed environments."""
    names = set()
    for stage in ('baseline_checks', 'prepublish_checks', 'checks'):
        for check in spec['policy'].get(stage, []):
            declarations = check.get('generated_directories', [])
            if not isinstance(declarations, list):
                raise ValueError('configured generated directories must be a list')
            for name in declarations:
                if not isinstance(name, str):
                    raise ValueError('configured generated directory must be a relative name')
                path = Path(name)
                if (not path.parts or path.as_posix() != name
                        or path.is_absolute() or any(part in {
                            '..', '.git', '.codex', '.agents'} for part in path.parts)):
                    raise ValueError('configured generated directory escaped its checkout')
                if path.name != '.venv':
                    names.add(name)
    return names


def workspace_dependency_directories(root: Path, *, policy: dict | None = None) -> dict[str, str]:
    """Bind nested dependencies to tracked layout, allowing authorized script edits."""
    import fnmatch
    import hashlib
    import json
    import subprocess

    import yaml

    from .delivery_broker import _git

    def tracked(name):
        path = root / name
        entry = _git(root, 'ls-tree', 'HEAD', '--', name)
        if not entry:
            return None
        if (not entry.startswith(('100644 blob ', '100755 blob '))
                or path.resolve(strict=True) != path or not path.is_file()):
            raise ValueError('workspace metadata is not fixed tracked regular data')
        content = path.read_bytes()
        committed = subprocess.check_output([
            'git', '-c', 'core.fsmonitor=false', '-C', str(root), 'show', 'HEAD:' + name],
            timeout=30)
        if len(content) > 1024 * 1024 or len(committed) > 1024 * 1024:
            raise ValueError('workspace metadata changed from its tracked declaration')
        if content != committed:
            from .delivery_source_scope import outside_scope

            permitted = (policy is not None and Path(name).name == 'package.json'
                         and not outside_scope(policy, [name], checkout=root))
            if permitted:
                current, original = json.loads(content), json.loads(committed)
                permitted = (isinstance(current, dict) and isinstance(original, dict)
                             and {k: v for k, v in current.items() if k != 'scripts'}
                             == {k: v for k, v in original.items() if k != 'scripts'})
            if not permitted:
                raise ValueError('workspace metadata changed from its tracked declaration')
        return committed

    metadata, patterns = {}, []
    manifest = tracked('package.json')
    if manifest is not None:
        declared = json.loads(manifest).get('workspaces', [])
        if isinstance(declared, dict):
            declared = declared.get('packages', [])
        if not isinstance(declared, list):
            raise ValueError('tracked workspace declaration is malformed')
        patterns.extend(declared)
        metadata['package.json'] = hashlib.sha256(manifest).hexdigest()
    workspace = tracked('pnpm-workspace.yaml')
    if workspace is not None:
        value = yaml.safe_load(workspace)
        if not isinstance(value, dict):
            raise ValueError('tracked workspace declaration is malformed')
        declared = value.get('packages', [])
        if not isinstance(declared, list):
            raise ValueError('tracked workspace declaration is malformed')
        patterns.extend(declared)
        metadata['pnpm-workspace.yaml'] = hashlib.sha256(workspace).hexdigest()
    if (len(patterns) > 256 or any(not isinstance(pattern, str) or not pattern
            or '{' in pattern or '}' in pattern
            or Path(pattern.lstrip('!')).is_absolute() or any(part in {
                '..', '.git', '.codex', '.agents'} for part in Path(pattern).parts)
            for pattern in patterns)):
        raise ValueError('tracked workspace declaration is malformed or unbounded')
    if not patterns:
        return {}
    packages = _git(root, 'ls-files', '-z', '--', ':(glob)**/package.json').split('\0')
    if len(packages) > 256:
        raise ValueError('tracked workspace package set is unbounded')
    def matches(project, pattern):
        # Workspace * is one path component; ** may span zero or more.
        parts, glob = project.split('/'), pattern.rstrip('/').split('/')
        pending, visited = [(0, 0)], set()
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            i, j = current
            if i == len(glob):
                if j == len(parts):
                    return True
            elif glob[i] == '**':
                pending.append((i + 1, j))
                if j < len(parts):
                    pending.append((i, j + 1))
            elif j < len(parts) and fnmatch.fnmatchcase(parts[j], glob[i]):
                pending.append((i + 1, j + 1))
        return False

    result = {}
    for name in filter(None, packages):
        project = Path(name).parent.as_posix()
        if (not any(matches(project, pattern)
                    for pattern in patterns if not pattern.startswith('!'))
                or any(matches(project, pattern[1:])
                       for pattern in patterns if pattern.startswith('!'))):
            continue
        content = tracked(name)
        if content is None:
            raise ValueError('workspace package has no committed regular manifest')
        result[project + '/node_modules'] = digest({
            'declarations': metadata, 'package': name,
            'package_sha256': hashlib.sha256(content).hexdigest()})
    return result
