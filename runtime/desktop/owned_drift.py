"""One explicit frozen ambient-state acknowledgement for an interrupted owned update."""

from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path

from owned_upgrade import NAME, locked, private, save, seal, sha, unrelated_seal

SIDECAR = "ambient-drift-acknowledgement.json"


def foreign_snapshot(codex, home):
    import tomllib

    from owned_plugin import command

    mcp = command(codex, home, "mcp", "list", "--json")
    plugins = command(codex, home, "plugin", "list", "--json")
    lines = command(codex, home, "plugin", "marketplace", "list").strip().splitlines()
    if (
        not isinstance(mcp, list)
        or not isinstance(plugins, dict)
        or not isinstance(plugins.get("installed"), list)
        or not lines
        or (
            lines != ["No plugin marketplaces in scope."]
            and lines[0].split() != ["MARKETPLACE", "ROOT"]
        )
    ):
        raise ValueError("unexpected public ambient inventory")
    marketplaces = {}
    for line in [] if lines == ["No plugin marketplaces in scope."] else lines[1:]:
        key, root = line.split(None, 1)
        if key in marketplaces:
            raise ValueError("duplicate ambient marketplace")
        marketplaces[key] = root.strip()
    settings = tomllib.loads((home / "config.toml").read_text())
    settings.get("mcp_servers", {}).pop(NAME, None)
    return {
        "settings": settings,
        "other_mcp": sorted(
            (entry for entry in mcp if entry.get("name") != NAME), key=lambda e: e["name"]
        ),
        "plugins": plugins,
        "marketplaces": marketplaces,
    }


def delta(before, after, path=()):
    """Typed values and absence are distinct; no ambient version equivalence."""
    if isinstance(before, dict) and isinstance(after, dict):
        result = []
        for key in sorted(before.keys() | after.keys()):
            if key not in before or key not in after:
                result.append(
                    {
                        "path": [*path, key],
                        "before": {"present": key in before, "value": before.get(key)},
                        "after": {"present": key in after, "value": after.get(key)},
                    }
                )
            else:
                result.extend(delta(before[key], after[key], (*path, key)))
        return result
    if seal(before) == seal(after):
        return []
    return [
        {
            "path": list(path),
            "before": {"present": True, "value": before},
            "after": {"present": True, "value": after},
        }
    ]


def content_index(root):
    """Complete bounded regular-file/metadata index, without following links."""
    if (
        not root.is_absolute()
        or any(p.is_symlink() for p in (root, *root.parents))
        or not root.is_dir()
    ):
        raise ValueError("ambient content root is absent or linked")
    files, directories, total = [], [], 0
    for directory, names, entries in os.walk(root, followlinks=False):
        names.sort()
        here = Path(directory)
        for path in [here, *(here / name for name in names)]:
            info = path.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
                raise ValueError("ambient content directory is linked or foreign")
        info = here.stat()
        directories.append(
            {
                "path": str(here.relative_to(root)),
                "uid": info.st_uid,
                "mode": oct(stat.S_IMODE(info.st_mode)),
            }
        )
        if len(directories) > 1024:
            raise ValueError("ambient content directory bound exceeded")
        for name in sorted(entries):
            path = here / name
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_nlink != 1
                or info.st_size > 8 * 1024 * 1024
            ):
                raise ValueError("ambient content file is linked, foreign or oversized")
            descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "rb") as stream:
                opened = os.fstat(stream.fileno())
                if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                    raise ValueError("ambient content identity changed")
                content = stream.read(8 * 1024 * 1024 + 1)
                finished = os.fstat(stream.fileno())
            stable = (
                "st_dev",
                "st_ino",
                "st_size",
                "st_mtime_ns",
                "st_ctime_ns",
                "st_uid",
                "st_nlink",
                "st_mode",
            )
            if len(content) != info.st_size or any(
                getattr(finished, key) != getattr(opened, key) for key in stable
            ):
                raise ValueError("ambient content changed while indexing")
            total += len(content)
            files.append(
                {
                    "path": str(path.relative_to(root)),
                    "sha256": sha(content),
                    "size": len(content),
                    "uid": info.st_uid,
                    "nlink": info.st_nlink,
                    "mode": oct(stat.S_IMODE(info.st_mode)),
                }
            )
            if len(files) > 512 or total > 16 * 1024 * 1024:
                raise ValueError("ambient content bound exceeded")
    files.sort(key=lambda e: Path(e["path"]).parts)
    directories.sort(key=lambda e: Path(e["path"]).parts)
    return {
        "root": str(root),
        "version": root.name,
        "file_count": len(files),
        "total_bytes": total,
        "files": files,
        "directories": directories,
        "unexpected_types": [],
        "all_owned_single_link_regular": True,
        "content_sha256": seal([{k: e[k] for k in ("path", "sha256", "size")} for e in files]),
    }


def _referenced(request, name):
    reference = request[name]
    if (
        not isinstance(reference, dict)
        or set(reference) != {"path", "sha256"}
        or not isinstance(reference["path"], str)
        or not Path(reference["path"]).is_absolute()
    ):
        raise ValueError("invalid ambient acknowledgement evidence reference")
    path = Path(reference["path"])
    if path.stat().st_size > 1024 * 1024:
        raise ValueError("ambient acknowledgement evidence exceeds its bound")
    content = private(path)
    if sha(content) != reference["sha256"]:
        raise ValueError("ambient acknowledgement referenced evidence changed")
    return json.loads(content)


def _material(request, manifest, codex, home):
    original_request = _referenced(request, "original_request")
    if original_request.get("command_id") != request["command_id"]:
        raise ValueError("ambient acknowledgement binds another original request")
    authority = _referenced(request, "authority")
    if (
        not isinstance(authority, dict)
        or authority.get("new_user_approval") is not False
        or not isinstance(authority.get("authority_source"), str)
        or not isinstance(authority.get("decision_owner"), str)
    ):
        raise ValueError("existing installation authority receipt is required")
    before = _referenced(request, "prior_snapshot")
    after = _referenced(request, "current_snapshot")
    keys = {"settings", "other_mcp", "plugins", "marketplaces"}
    if (
        not isinstance(before, dict)
        or not isinstance(after, dict)
        or set(before) != keys
        or set(after) != keys
    ):
        raise ValueError("invalid frozen ambient snapshot")
    if (
        unrelated_seal(
            {k: before[k] for k in ("settings", "other_mcp")}, manifest["unrelated_sha256"]
        )
        != manifest["unrelated_sha256"]
    ):
        raise ValueError("ambient prior snapshot does not authenticate the original seal")
    changes = delta(before, after)
    if not changes or len(changes) > 128 or seal(changes) != request["delta_sha256"]:
        raise ValueError("ambient acknowledgement delta changed or exceeds its bound")
    if unrelated_seal(foreign_snapshot(codex, home)) != unrelated_seal(after):
        raise ValueError("ambient current public snapshot changed")
    indexes = request["content_indexes"]
    if not isinstance(indexes, list) or not 1 <= len(indexes) <= 8:
        raise ValueError("ambient acknowledgement requires bounded content evidence")
    roots = set()
    allowed_roots = set()
    for plugin in after["plugins"]["installed"]:
        components = [plugin.get(key) for key in ("marketplaceName", "name", "version")]
        if (
            any(
                not isinstance(value, str) or value in {".", ".."} or "/" in value
                for value in components
            )
            or plugin.get("pluginId") == "devflow@devflow-local"
        ):
            raise ValueError("invalid foreign installed plugin identity")
        allowed_roots.add(home / "plugins/cache" / Path(*components))
    for reference in indexes:
        index = _referenced({"index": reference}, "index")
        root = Path(index["root"])
        if root in roots or root not in allowed_roots or content_index(root) != index:
            raise ValueError("ambient acknowledged content changed")
        roots.add(root)
    prior = {p["pluginId"]: p for p in before["plugins"]["installed"]}
    for plugin in after["plugins"]["installed"]:
        if plugin != prior.get(plugin["pluginId"]) and plugin.get("installed"):
            root = (
                home
                / "plugins/cache"
                / plugin["marketplaceName"]
                / plugin["name"]
                / plugin["version"]
            )
            if root not in roots:
                raise ValueError("changed installed plugin lacks complete current content evidence")
    for prefix in ("old", "new"):
        if (
            sha(private(Path(manifest[f"{prefix}_config_path"])))
            != manifest[f"{prefix}_config_sha256"]
        ):
            raise ValueError("frozen owned configuration changed")
    source = Path(__file__).parent / NAME / "SKILL.md"
    if sha(source.read_bytes()) != manifest["new_skill_sha256"]:
        raise ValueError("frozen owned skill source changed")
    return unrelated_seal({k: after[k] for k in ("settings", "other_mcp")})


def guard(codex, home, manifest_path, manifest):
    path = manifest_path.parent / SIDECAR
    if not path.exists():
        return manifest["unrelated_sha256"]
    acknowledgement = json.loads(private(path))
    raw = private(Path(acknowledgement["request_path"]))
    if sha(raw) != acknowledgement["request_sha256"]:
        raise ValueError("immutable ambient acknowledgement request changed")
    request = json.loads(raw)
    original_text = acknowledgement["original_manifest_json"]
    original = json.loads(original_text)
    if (
        sha(original_text.encode()) != request["original_manifest_sha256"]
        or manifest.get("state") not in {"prepared", "unknown", "applied", "rolled_back"}
        or request["original_unrelated_sha256"] != original["unrelated_sha256"]
        or {k: v for k, v in manifest.items() if k != "state"}
        != {k: v for k, v in original.items() if k != "state"}
    ):
        raise ValueError("original owned journal changed after ambient acknowledgement")
    return _material(request, original, codex, home)


def acknowledge(codex, home, executable, config, source_skill, request_path):
    with locked(home):
        if not request_path.is_absolute() or request_path.stat().st_size > 64 * 1024:
            raise ValueError('ambient acknowledgement request must be absolute and bounded')
        raw = private(request_path)
        request = json.loads(raw)
        required = {
            "command_id",
            "original_request",
            "original_manifest_sha256",
            "original_unrelated_sha256",
            "authority",
            "prior_snapshot",
            "current_snapshot",
            "delta_sha256",
            "content_indexes",
        }
        if (
            len(raw) > 64 * 1024
            or not isinstance(request, dict)
            or set(request) != required
            or not isinstance(request["command_id"], str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", request["command_id"])
        ):
            raise ValueError("invalid owned ambient acknowledgement request")
        manifest_path = (
            home / ".devflow-local-delivery-upgrades" / request["command_id"] / "manifest.json"
        )
        original_raw = private(manifest_path)
        manifest = json.loads(original_raw)
        path = manifest_path.parent / SIDECAR
        if path.exists():
            stored = json.loads(private(path))
            if stored["request_sha256"] != sha(raw) or stored["request_path"] != str(request_path):
                raise ValueError("original command already has another ambient acknowledgement")
            guard(codex, home, manifest_path, manifest)
            return {"existing": True, "acknowledgement": str(path), "state": "acknowledged"}
        original_request = _referenced(request, "original_request")
        binding = {
            **original_request,
            "home": str(home),
            "executable": str(executable),
            "config": str(config),
            "config_sha256": sha(private(config)),
            "new_skill_sha256": sha(source_skill.read_bytes()),
        }
        if (
            manifest["state"] != "unknown"
            or sha(original_raw) != request["original_manifest_sha256"]
            or manifest["unrelated_sha256"] != request["original_unrelated_sha256"]
            or manifest["command_digest"] != seal(binding)
            or manifest["codex_home"] != str(home)
        ):
            raise ValueError("ambient acknowledgement original unknown command changed")
        expected = _material(request, manifest, codex, home)
        from owned_upgrade import snapshot

        current, observed = snapshot(codex, home, expected)
        skill = home / "skills" / NAME / "SKILL.md"
        if (
            current not in (manifest["before"], manifest["after"])
            or observed != expected
            or sha(skill.read_bytes())
            not in (manifest["old_skill_sha256"], manifest["new_skill_sha256"])
            or sha(private(Path(manifest["old_config_path"]))) != manifest["old_config_sha256"]
        ):
            raise ValueError("original owned installation authority changed")
        save(
            path,
            {
                "request_path": str(request_path),
                "request_sha256": sha(raw),
                "original_manifest_json": original_raw.decode(),
            },
        )
        guard(codex, home, manifest_path, manifest)
        return {"existing": False, "acknowledgement": str(path), "state": "acknowledged"}
