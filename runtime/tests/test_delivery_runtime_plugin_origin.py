"""Historical source evidence supplements exact live package/transport custody."""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import subprocess
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


def test_exact_older_git_package_and_complete_current_custody(original):
    case = original
    proof = owner._origin(case.supplied, dict(case.encoded))
    assert proof["request"] == case.witness
    assert hashlib.sha256(base64.b64decode(case.encoded[SKILL])).hexdigest() == (
        "76672f435fb2fd0ba9ea1971a3403895b4d6581990e094752bcea673e9548aa3"
    )
    assert (
        base64.b64decode(case.encoded[SKILL])
        != (owner.source_root() / "runtime/desktop/devflow-local-delivery/SKILL.md").read_bytes()
    )
    assert owner._package_tree(case.cache, owner.PLUGIN_FILES)["files"] == case.encoded
    assert owner._original_tree(str(case.market), case.encoded, case.package)
    owner._plugin_metadata(case.snapshot, case.encoded, str(case.market), case.old, case.config)


@pytest.mark.parametrize("part", ["cache", "package", "catalog"])
def test_complete_membership_refuses_extra_empty_directory(original, part):
    case = original
    root = {
        "cache": case.cache,
        "package": case.market / "plugins/devflow",
        "catalog": case.market / ".agents/plugins",
    }[part]
    (root / "unregistered-empty").mkdir()
    with pytest.raises(ValueError, match="extra or duplicate"):
        if part == "cache":
            owner._package_tree(case.cache, owner.PLUGIN_FILES)
        else:
            owner._original_tree(str(case.market), case.encoded, case.package)


@pytest.mark.parametrize("defect", ["symlink", "hardlink", "fifo", "mode", "missing"])
def test_package_members_require_protected_single_name_regular_objects(original, tmp_path, defect):
    path = original.cache / SKILL
    if defect == "mode":
        path.chmod(0o666)
    elif defect == "hardlink":
        os.link(path, tmp_path / "another-name")
    else:
        raw = path.read_bytes()
        path.unlink()
        if defect == "symlink":
            target = tmp_path / "linked-skill"
            target.write_bytes(raw)
            path.symlink_to(target)
        elif defect == "fifo":
            os.mkfifo(path, 0o600)
    with pytest.raises((ValueError, OSError)):
        owner._package_tree(original.cache, owner.PLUGIN_FILES)


@pytest.mark.parametrize("defect", ["skill", "command", "config", "extra", "future", "missing"])
def test_source_witness_cannot_authenticate_changed_or_foreign_bytes(original, defect):
    case = original
    supplied, expected = copy.deepcopy(case.supplied), dict(case.encoded)
    if defect == "future":
        supplied["plugin_origin"]["source_revision"] = os.fsdecode(
            owner._git_data("rev-parse", "HEAD")
        ).strip()
    elif defect == "missing":
        supplied["plugin_origin"]["source_revision"] = "0" * 40
    elif defect == "extra":
        supplied["plugin_origin"]["files_sha256"]["extra.txt"] = "a" * 64
    else:
        name = SKILL if defect == "skill" else ".mcp.json"
        raw = base64.b64decode(expected[name])
        if defect == "skill":
            raw += b"unacknowledged skill change\n"
        else:
            value = json.loads(raw)
            server = value["mcpServers"]["devflow-local-delivery"]
            if defect == "command":
                server["command"] += "-foreign"
            else:
                server["args"][1] += "-foreign"
            raw = case.package.json_bytes(value)
        supplied["plugin_origin"]["files_sha256"][name] = hashlib.sha256(raw).hexdigest()
        # A foreign skill can be present in both live copies. Its witness must
        # still reproduce the named immutable source, never bless matching drift.
        if defect == "skill":
            for root in (case.cache, case.market / "plugins/devflow"):
                (root / name).write_bytes(raw)
    with pytest.raises(ValueError):
        owner._origin(supplied, expected)


@pytest.mark.parametrize(
    "defect",
    [
        "disabled",
        "uninstalled",
        "source",
        "market",
        "version",
        "duplicate",
        "args",
        "env",
        "metadata",
    ],
)
def test_exact_public_plugin_and_effective_transport_are_required(original, defect):
    case = original
    changed = copy.deepcopy(case.snapshot)
    if defect == "disabled":
        changed["plugin"]["enabled"] = False
    elif defect == "uninstalled":
        changed["plugin"]["installed"] = False
    elif defect == "source":
        changed["plugin"]["source"]["path"] += "-foreign"
    elif defect == "market":
        changed["marketplace"] += "-foreign"
    elif defect == "version":
        changed["plugin"]["version"] = "other"
    elif defect == "duplicate":
        changed["direct"] = changed["plugin_mcp"]
    elif defect == "args":
        changed["plugin_mcp"]["transport"]["args"].append("--other")
    elif defect == "env":
        changed["plugin_mcp"]["transport"]["env"] = {"UNACKNOWLEDGED": "1"}
    else:
        changed["plugin"]["unacknowledged"] = True
    with pytest.raises(ValueError):
        owner._plugin_metadata(
            changed,
            case.encoded,
            str(case.market),
            case.old,
            case.config,
            original=case.snapshot,
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


def test_original_tree_byte_and_generation_drift_refuses_before_cli_effects(original):
    case = original
    original_tree = owner._original_tree(str(case.market), case.encoded, case.package)
    plan = {
        "before": case.snapshot,
        "before_files": case.encoded,
        "original_tree": original_tree,
        "origin": owner._origin(case.supplied, dict(case.encoded)),
    }
    skill = case.market / "plugins/devflow" / SKILL
    raw = skill.read_bytes()
    skill.write_bytes(raw)  # Same bytes still cannot replace the sealed original generation.
    with pytest.raises(ValueError, match="custody changed"):
        owner._plugin_state(plan)
