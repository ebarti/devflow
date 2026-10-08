"""Frozen public-index host dependency identity."""

from __future__ import annotations

import tomllib

import pytest

from devflow_temporal import runtime_dependencies as dependencies


@pytest.mark.parametrize("change", ["cli_version", "git_source"])
def test_mixed_or_unlocked_runtime_dependencies_are_rejected(tmp_path, change):
    lock = (dependencies.RUNTIME / "uv.lock").read_text()
    if change == "cli_version":
        packages = {package['name']: package for package in tomllib.loads(lock)['package']}
        cli_version = packages['openai-codex-cli-bin']['version']
        lock = lock.replace(f'name = "openai-codex-cli-bin"\nversion = "{cli_version}"',
                            'name = "openai-codex-cli-bin"\nversion = "0.0.0"')
    else:
        lock = lock.replace('source = { registry = "https://pypi.org/simple" }',
                            'source = { git = "https://example.invalid/kit" }', 1)
    (tmp_path / "uv.lock").write_text(lock)
    with pytest.raises(ValueError, match="versions differ|public-index"):
        dependencies.locked_dependency_identity(tmp_path)
