"""Bounded immutable ambient acknowledgements for the same interrupted owned update."""

from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path

from owned_upgrade import NAME, locked, private, seal, sha, unrelated_seal

SIDECAR = "ambient-drift-acknowledgement.json"
MAX_ACKNOWLEDGEMENTS = 4
STATES = {"prepared", "unknown", "applied", "rolled_back"}
REQUEST_FIELDS = {
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
SUCCESSOR_FIELDS = {"predecessor_sha256", "expected_manifest_sha256", "expected_manifest_state"}


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
    if name in {"authority", "index"}:
        if any(parent.is_symlink() for parent in path.parents):
            raise ValueError("public ambient evidence reference is linked")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            info = os.fstat(stream.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) not in {0o600, 0o644}
                or info.st_size > 1024 * 1024
            ):
                raise ValueError(
                    "public ambient evidence must be an owned regular immutable reference"
                )
            content = stream.read(1024 * 1024 + 1)
    else:
        content = private(path)
    if sha(content) != reference["sha256"]:
        raise ValueError("ambient acknowledgement referenced evidence changed")
    return json.loads(content)


def _material(request, manifest, codex, home, *, successor=False, live=True):
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
    if not successor and (
        unrelated_seal(
            {k: before[k] for k in ("settings", "other_mcp")}, manifest["unrelated_sha256"]
        )
        != manifest["unrelated_sha256"]
    ):
        raise ValueError("ambient prior snapshot does not authenticate the original seal")
    changes = delta(before, after)
    if not changes or len(changes) > 128 or seal(changes) != request["delta_sha256"]:
        raise ValueError("ambient acknowledgement delta changed or exceeds its bound")
    if live and unrelated_seal(foreign_snapshot(codex, home)) != unrelated_seal(after):
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
        if root in roots or root not in allowed_roots or (live and content_index(root) != index):
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
    return unrelated_seal({k: after[k] for k in ("settings", "other_mcp")})


def _live_owned(codex, home, manifest, expected, pointer=None, skill_sha256=None):
    from owned_upgrade import snapshot

    current, unrelated = snapshot(codex, home, expected)
    for prefix in ("old", "new"):
        if (
            sha(private(Path(manifest[f"{prefix}_config_path"])))
            != manifest[f"{prefix}_config_sha256"]
        ):
            raise ValueError("frozen owned configuration changed")
    source = Path(__file__).parent / NAME / "SKILL.md"
    if sha(source.read_bytes()) != manifest["new_skill_sha256"]:
        raise ValueError("frozen owned skill source changed")
    target = home / "skills" / NAME / "SKILL.md"
    if any(path.is_symlink() for path in (target, *target.parents)):
        raise ValueError("live owned skill path is linked")
    descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_nlink != 1
            or info.st_size > 1024 * 1024
        ):
            raise ValueError("live owned skill identity changed")
        content = stream.read(1024 * 1024 + 1)
    pointers = (pointer,) if pointer is not None else (manifest["before"], manifest["after"])
    skills = (
        (skill_sha256,)
        if skill_sha256 is not None
        else (manifest["old_skill_sha256"], manifest["new_skill_sha256"])
    )
    if unrelated != expected or current not in pointers or sha(content) not in skills:
        raise ValueError("public update readback disagrees or live owned installation changed")


def _request(path):
    if not path.is_absolute() or path.stat().st_size > 64 * 1024:
        raise ValueError("ambient acknowledgement request must be absolute and bounded")
    raw = private(path)
    request = json.loads(raw)
    if (
        len(raw) > 64 * 1024
        or not isinstance(request, dict)
        or set(request) not in (REQUEST_FIELDS, REQUEST_FIELDS | SUCCESSOR_FIELDS)
        or not isinstance(request["command_id"], str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", request["command_id"])
    ):
        raise ValueError("invalid owned ambient acknowledgement request")
    return raw, request


def _same_journal(original, current):
    if current.get("state") not in STATES or {k: v for k, v in current.items() if k != "state"} != {
        k: v for k, v in original.items() if k != "state"
    }:
        raise ValueError("original owned journal changed after ambient acknowledgement")


def _receipt_path(directory, sequence):
    return directory / (
        SIDECAR if sequence == 1 else f"ambient-drift-acknowledgement-{sequence}.json"
    )


def _chain(codex, home, manifest_path, manifest):
    """Authenticate immutable history without requiring old foreign state to stay live."""
    directory = manifest_path.parent
    paths = {p for p in directory.iterdir() if p.name.startswith("ambient-drift-acknowledgement")}
    allowed = {_receipt_path(directory, n) for n in range(1, MAX_ACKNOWLEDGEMENTS + 1)}
    if paths - allowed or paths != {_receipt_path(directory, n) for n in range(1, len(paths) + 1)}:
        raise ValueError("ambient acknowledgement chain is missing, ambiguous or exceeds its bound")
    chain, original = [], None
    for sequence in range(1, len(paths) + 1):
        path = _receipt_path(directory, sequence)
        if path.stat().st_size > 256 * 1024:
            raise ValueError("ambient acknowledgement receipt exceeds its bound")
        receipt_raw = private(path)
        receipt = json.loads(receipt_raw)
        if receipt_raw != (json.dumps(receipt, sort_keys=True, indent=2) + "\n").encode():
            raise ValueError("immutable ambient acknowledgement receipt changed")
        expected_keys = (
            {"request_path", "request_sha256", "original_manifest_json"}
            if sequence == 1
            else {"request_path", "request_sha256", "predecessor_sha256", "admitted_manifest_json"}
        )
        if not isinstance(receipt, dict) or set(receipt) != expected_keys:
            raise ValueError("immutable ambient acknowledgement receipt changed")
        raw, request = _request(Path(receipt["request_path"]))
        if sha(raw) != receipt["request_sha256"]:
            raise ValueError("immutable ambient acknowledgement request changed")
        if sequence == 1:
            original_text = receipt["original_manifest_json"]
            original = json.loads(original_text)
            if (
                set(request) != REQUEST_FIELDS
                or original["state"] != "unknown"
                or sha(original_text.encode()) != request["original_manifest_sha256"]
                or request["original_unrelated_sha256"] != original["unrelated_sha256"]
                or original["codex_home"] != str(home)
            ):
                raise ValueError("ambient acknowledgement original unknown command changed")
        else:
            prior = chain[-1]
            admitted_text = receipt["admitted_manifest_json"]
            admitted = json.loads(admitted_text)
            _same_journal(original, admitted)
            if (
                set(request) != REQUEST_FIELDS | SUCCESSOR_FIELDS
                or request["predecessor_sha256"] != prior["sha256"]
                or receipt["predecessor_sha256"] != prior["sha256"]
                or sha(admitted_text.encode()) != request["expected_manifest_sha256"]
                or admitted["state"] != request["expected_manifest_state"]
                or request["prior_snapshot"] != prior["request"]["current_snapshot"]
                or request["authority"]["sha256"] == prior["request"]["authority"]["sha256"]
                or any(
                    request[key] != chain[0]["request"][key]
                    for key in (
                        "command_id",
                        "original_request",
                        "original_manifest_sha256",
                        "original_unrelated_sha256",
                    )
                )
            ):
                raise ValueError("ambient acknowledgement successor predecessor or journal changed")
        _material(request, original, codex, home, successor=sequence > 1, live=False)
        chain.append({"path": path, "sha256": sha(receipt_raw), "request": request})
    if chain:
        _same_journal(original, manifest)
    return chain, original


def _current_material(codex, home, chain, original):
    request = chain[-1]["request"]
    expected = _material(request, original, codex, home, successor=len(chain) > 1)
    # Successors retain current content evidence for every previously indexed
    # installed plugin, including those whose inventory did not change again.
    current = _referenced(request, "current_snapshot")
    roots = {
        str(home / "plugins/cache" / p["marketplaceName"] / p["name"] / p["version"])
        for p in current["plugins"]["installed"]
        if p.get("installed")
    }
    latest_roots = {_referenced({"index": r}, "index")["root"] for r in request["content_indexes"]}
    for predecessor in chain[:-1]:
        for reference in predecessor["request"]["content_indexes"]:
            root = _referenced({"index": reference}, "index")["root"]
            if root in roots and root not in latest_roots:
                raise ValueError("successor lacks retained current content evidence")
    return expected


def guard(codex, home, manifest_path, manifest, *, pointer=None, skill_sha256=None):
    chain, original = _chain(codex, home, manifest_path, manifest)
    expected = (
        _current_material(codex, home, chain, original) if chain else manifest["unrelated_sha256"]
    )
    _live_owned(codex, home, original or manifest, expected, pointer, skill_sha256)
    return expected


def _append(path, value):
    """Atomic exclusive append: existing receipt bytes are never replaced."""
    content = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    temporary = path.with_name(f".ambient-append-{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
    finally:
        temporary.unlink(missing_ok=True)


def _response(path, existing, length):
    return {
        "existing": existing,
        "acknowledgement": str(path),
        "state": "acknowledged",
        "chain_length": length,
        "max_acknowledgements": MAX_ACKNOWLEDGEMENTS,
    }


def acknowledge(codex, home, executable, config, source_skill, request_path):
    with locked(home):
        raw, request = _request(request_path)
        manifest_path = (
            home / ".devflow-local-delivery-upgrades" / request["command_id"] / "manifest.json"
        )
        manifest_raw = private(manifest_path)
        manifest = json.loads(manifest_raw)
        chain, original = _chain(codex, home, manifest_path, manifest)
        for entry in chain:
            receipt = json.loads(private(entry["path"]))
            if receipt["request_path"] == str(request_path) and receipt["request_sha256"] == sha(
                raw
            ):
                guard(codex, home, manifest_path, manifest)
                return _response(entry["path"], True, len(chain))
        if len(chain) == MAX_ACKNOWLEDGEMENTS:
            raise ValueError("ambient acknowledgement chain is exhausted")
        original_request = _referenced(request, "original_request")
        binding = {
            **original_request,
            "home": str(home),
            "executable": str(executable),
            "config": str(config),
            "config_sha256": sha(private(config)),
            "new_skill_sha256": sha(source_skill.read_bytes()),
        }
        original = original or manifest
        if (
            original["command_digest"] != seal(binding)
            or original["codex_home"] != str(home)
            or request["original_unrelated_sha256"] != original["unrelated_sha256"]
        ):
            raise ValueError("ambient acknowledgement original unknown command changed")
        if chain:
            prior = chain[-1]
            if (
                set(request) != REQUEST_FIELDS | SUCCESSOR_FIELDS
                or request["predecessor_sha256"] != prior["sha256"]
                or request["expected_manifest_sha256"] != sha(manifest_raw)
                or request["expected_manifest_state"] != manifest["state"]
                or request["prior_snapshot"] != prior["request"]["current_snapshot"]
                or request["authority"]["sha256"] == prior["request"]["authority"]["sha256"]
                or any(
                    request[key] != chain[0]["request"][key]
                    for key in (
                        "command_id",
                        "original_request",
                        "original_manifest_sha256",
                        "original_unrelated_sha256",
                    )
                )
            ):
                raise ValueError(
                    "ambient successor must bind the immediate predecessor and current journal"
                )
        elif (
            set(request) != REQUEST_FIELDS
            or manifest["state"] != "unknown"
            or sha(manifest_raw) != request["original_manifest_sha256"]
        ):
            raise ValueError("ambient acknowledgement original unknown command changed")
        # Two independent complete reads, including bounded content, must agree
        # with the explicit request. No automatic capture or successor renewal.
        prospective = [*chain, {"request": request}]
        for _ in range(2):
            expected = _current_material(codex, home, prospective, original)
        _live_owned(codex, home, original, expected)
        if private(manifest_path) != manifest_raw:
            raise ValueError("current owned journal changed during acknowledgement")
        path = _receipt_path(manifest_path.parent, len(chain) + 1)
        receipt = {"request_path": str(request_path), "request_sha256": sha(raw)}
        if chain:
            receipt.update(
                predecessor_sha256=chain[-1]["sha256"], admitted_manifest_json=manifest_raw.decode()
            )
        else:
            receipt["original_manifest_json"] = manifest_raw.decode()
        _append(path, receipt)
        guard(codex, home, manifest_path, manifest)
        return _response(path, False, len(chain) + 1)
