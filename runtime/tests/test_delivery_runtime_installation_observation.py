"""Owning readers bind one private evidence generation and exact legacy entrypoints."""

import shlex
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_resources import spec

from devflow_temporal import delivery_resources as resources
from devflow_temporal import delivery_runtime_installation as owner


def observed(tmp_path):
    supplied = spec(tmp_path)
    registry = resources.RunResources(supplied)
    journal = Path(supplied["state_dir"]) / "attempts/owned/native-process.json"
    resources.write_private(
        journal,
        {
            "phase": "finished",
            "monitoring_complete": True,
            "result": {"cleanup": "observed-native-confirmed"},
            "owned": {},
            "ports": [],
        },
    )
    for name in ("native-process.lock", "native-monitor.lock"):
        journal.with_name(name).write_bytes(b"")
        journal.with_name(name).chmod(0o600)
    registry.process(journal)
    return supplied, registry, journal


def test_native_observation_refuses_manifest_changed_after_locked_parse(tmp_path, monkeypatch):
    supplied, registry, _journal = observed(tmp_path)
    original = resources.read_private
    changed = []

    def parsed(path):
        value = original(path)
        if Path(path) == registry.manifest and not changed:
            replacement = {**value, "observed-different-generation": True}
            changed.append(True)
            resources.write_private(registry.manifest, replacement)
        return value

    monkeypatch.setattr(resources, "read_private", parsed)
    with ExitStack() as leases, pytest.raises(ValueError, match="manifest changed"):
        owner._native_observation(supplied, leases)


@pytest.mark.parametrize(
    "shape,accepted",
    [
        ("console", True),
        ("direct", True),
        ("module", True),
        ("isolated", True),
        ("base-console", True),
        ("dummy-console", False),
        ("dummy-module", False),
        ("extra", False),
        ("foreign-owner", False),
        ("python-c", False),
    ],
)
def test_original_actor_inventory_exempts_only_actual_adapter_entrypoint(
    tmp_path,
    monkeypatch,
    shape,
    accepted,
):
    root = tmp_path / "original-runtime"
    binary = root / "runtime/.venv/lib/python3.12/site-packages/codex_cli_bin/bin/codex"
    python, mcp = (
        str(root / "runtime/.venv/bin/python"),
        str(root / "runtime/.venv/bin/devflow-delivery-mcp"),
    )
    base, config = str(tmp_path / "base/python3.12"), str(tmp_path / "owner.json")
    tail = ["--config", config]
    variants = {
        "console": [python, mcp, *tail],
        "direct": [mcp, *tail],
        "module": [python, "-m", "devflow_temporal.delivery_mcp", *tail],
        "isolated": [python, "-I", "-u", "-m", "devflow_temporal.delivery_mcp", *tail],
        "base-console": [base, mcp, *tail],
        "dummy-console": [python, str(tmp_path / "different-controller.py"), mcp, *tail],
        "dummy-module": [
            python,
            str(tmp_path / "different-controller.py"),
            "--dummy",
            "-m",
            "devflow_temporal.delivery_mcp",
            *tail,
        ],
        "extra": [python, mcp, *tail, "--dummy"],
        "foreign-owner": [python, mcp, "--config", str(tmp_path / "other-owner")],
        "python-c": [python, "-c", "pass", "-m", "devflow_temporal.delivery_mcp", *tail],
    }
    receipt = {
        "owner_path": config,
        "runs": [
            {
                "original_spec": {
                    "policy": {"codex_bin": str(binary), "native_identity": {"python": base}}
                }
            }
        ],
    }
    monkeypatch.setattr(
        owner.subprocess,
        "run",
        lambda *a, **k: SimpleNamespace(stdout="600001 " + shlex.join(variants[shape]) + "\n"),
    )
    monkeypatch.setattr(owner, "_process_views", lambda: {600001: {"owned-start-identity"}})
    if accepted:
        owner._original_programs_absent(receipt)
    else:
        with pytest.raises(ValueError, match="still live"):
            owner._original_programs_absent(receipt)
