"""Frozen public-index host dependency identity."""

from __future__ import annotations

import pytest

from devflow_temporal import runtime_dependencies as dependencies


@pytest.mark.parametrize("change", ["cli_version", "git_source"])
def test_mixed_or_unlocked_runtime_dependencies_are_rejected(tmp_path, change):
    lock = (dependencies.RUNTIME / "uv.lock").read_text()
    if change == "cli_version":
        lock = lock.replace('name = "openai-codex-cli-bin"\nversion = "0.160.0"',
                            'name = "openai-codex-cli-bin"\nversion = "0.159.0"')
    else:
        lock = lock.replace('source = { registry = "https://pypi.org/simple" }',
                            'source = { git = "https://example.invalid/kit" }', 1)
    (tmp_path / "uv.lock").write_text(lock)
    with pytest.raises(ValueError, match="versions differ|public-index"):
        dependencies.locked_dependency_identity(tmp_path)
