"""Historical source evidence supplements exact live package/transport custody."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_runtime_registrations as owner

REVISION = "27aa49e188d9a8b788169f69a066dc913061fb94"
BASELINE = "824f00f9cb664e6e61285589bf3c258e651d6653"
SKILL = "skills/devflow-local-delivery/SKILL.md"


@pytest.fixture
def original(tmp_path):
    owned, package = owner._desktop()
    config = tmp_path / "owner.json"
    config.write_text("{}")
    config.chmod(0o600)
    old = str(tmp_path / "absent-original/runtime/.venv/bin/devflow-delivery-mcp")
    files = package.package_files(owner.source_root() / "runtime", config, "codex")
    mcp = json.loads(files[".mcp.json"])
    mcp["mcpServers"]["devflow-local-delivery"]["command"] = old
    files[".mcp.json"] = package.json_bytes(mcp)
    files[SKILL] = owner._git_blob(REVISION, "runtime/desktop/devflow-local-delivery/SKILL.md")
    market, cache = tmp_path / "marketplace", tmp_path / "cache"
    catalog = package.json_bytes(
        {
            "name": "devflow-local",
            "interface": {"displayName": "Devflow Local"},
            "plugins": [package.ENTRY],
        }
    )
    for root in (market / "plugins/devflow", cache):
        for name, raw in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            path.write_bytes(raw)
            path.chmod(0o600)
    path = market / owner.CATALOG
    path.parent.mkdir(parents=True, mode=0o700)
    path.write_bytes(catalog)
    path.chmod(0o600)
    witness = {
        "schema": owner.PLUGIN_ORIGIN,
        "source_revision": REVISION,
        "marketplace": str(market),
        "catalog_sha256": hashlib.sha256(catalog).hexdigest(),
        "files_sha256": {name: hashlib.sha256(raw).hexdigest() for name, raw in files.items()},
    }
    supplied = {"plugin_origin": witness, "baseline_revision": BASELINE}
    encoded = {name: base64.b64encode(raw).decode() for name, raw in files.items()}
    snapshot = {
        "direct": None,
        "marketplace": str(market),
        "unrelated_sha256": "f" * 64,
        "plugin": {
            "pluginId": "devflow@devflow-local",
            "name": "devflow",
            "version": "0.1.0",
            "enabled": True,
            "installed": True,
            "marketplaceName": "devflow-local",
            "source": {"source": "local", "path": str(market / "plugins/devflow")},
            "marketplaceSource": {"sourceType": "local", "source": str(market)},
        },
        "plugin_mcp": {
            "name": "devflow-local-delivery",
            "enabled": True,
            "transport": copy.deepcopy(mcp["mcpServers"]["devflow-local-delivery"]),
        },
    }
    return SimpleNamespace(
        owned=owned,
        package=package,
        config=config,
        old=old,
        market=market,
        cache=cache,
        witness=witness,
        supplied=supplied,
        encoded=encoded,
        snapshot=snapshot,
    )


def test_source_witness_ignores_replacement_objects_and_refuses_grafts(
    original, tmp_path, monkeypatch
):
    case = original
    clone = tmp_path / "source"
    subprocess.run(
        [
            "git",
            "clone",
            "--quiet",
            "--no-hardlinks",
            "--dissociate",
            str(owner.source_root()),
            str(clone),
        ],
        check=True,
    )
    current = subprocess.check_output(["git", "-C", str(clone), "rev-parse", "HEAD"]).strip()
    subprocess.run(["git", "-C", str(clone), "replace", REVISION, os.fsdecode(current)], check=True)
    monkeypatch.setattr(owner, "source_root", lambda: clone)
    proof = owner._origin(case.supplied, dict(case.encoded))
    assert proof["request"]["source_revision"] == REVISION
    graft = clone / ".git/info/grafts"
    graft.write_text(REVISION + "\n")
    with pytest.raises(ValueError, match="graft interpretation"):
        owner._origin(case.supplied, dict(case.encoded))


def target_plan(case, tmp_path):
    target = tmp_path / "target-marketplace"
    shutil.copytree(case.market, target)
    after = dict(case.encoded)
    mcp = json.loads(base64.b64decode(after[".mcp.json"]))
    mcp["mcpServers"]["devflow-local-delivery"]["command"] = (
        "/owned/permanent/runtime/.venv/bin/devflow-delivery-mcp"
    )
    raw = case.package.json_bytes(mcp)
    after[".mcp.json"] = base64.b64encode(raw).decode()
    (target / "plugins/devflow/.mcp.json").write_bytes(raw)
    cli = tmp_path / "host-cli"
    cli.write_bytes(b"owned inert CLI identity\n")
    return {
        "before": case.snapshot,
        "before_files": case.encoded,
        "after_files": after,
        "new_marketplace": str(target),
        "progress": str(tmp_path / "public-progress.json"),
        "original_tree": owner._original_tree(str(case.market), case.encoded, case.package),
        "origin": owner._origin(case.supplied, dict(case.encoded)),
        "cli": str(cli),
        "cli_sha256": hashlib.sha256(cli.read_bytes()).hexdigest(),
        "cache": str(case.cache),
        "codex_home": str(tmp_path),
    }


def test_target_is_rechecked_before_the_next_public_effect(original, tmp_path, monkeypatch):
    case = original
    plan = target_plan(case, tmp_path)
    observed = copy.deepcopy(case.snapshot)
    monkeypatch.setattr(
        owner, "_plugin_state", lambda _: (observed, case.encoded if observed["plugin"] else None)
    )
    calls = []

    def effect(*args):
        calls.append(args)
        observed["plugin"] = None
        (Path(plan["new_marketplace"]) / "plugins/devflow/extra-empty").mkdir()

    monkeypatch.setattr(case.owned, "command", effect)
    with pytest.raises(ValueError, match="extra or duplicate"):
        owner.plugin_apply(plan, lambda: None)
    assert len(calls) == 1
    assert calls[0][-4:] == ("plugin", "remove", "devflow@devflow-local", "--json")
