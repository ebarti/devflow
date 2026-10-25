"""Guarded owned launcher/helper exchange and public plugin location update.

The service installation journal owns these effects. No Codex cache, settings,
marketplace registration or ProjectSync file is written directly.
"""

from __future__ import annotations

import base64
import ctypes
import hashlib
import importlib
import importlib.util
import json
import os
import plistlib
import shlex
import stat
import sys
import tempfile
from pathlib import Path

from .contracts import digest
from .delivery_runtime_location import source_root


def _desktop():
    folder = source_root() / "runtime/desktop"
    for name in ("owned_upgrade", "owned_drift", "package_plugin", "owned_plugin"):
        existing = sys.modules.get(name)
        if existing and Path(existing.__file__).resolve() != folder / (name + ".py"):
            raise ValueError("installer helper module was imported from a foreign location")
    sys.path.insert(0, str(folder))
    try:
        return importlib.import_module("owned_plugin"), importlib.import_module("package_plugin")
    finally:
        sys.path.remove(str(folder))


def _read(path):
    path = Path(path)
    if path.resolve(strict=True) != path:
        raise ValueError("owned registration contains an alias")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.getuid()
            or before.st_nlink != 1
            or before.st_mode & 0o022
            or before.st_size > 4 * 1024 * 1024
        ):
            raise ValueError("registration is not a bounded owned protected file")
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            raw = stream.read(4 * 1024 * 1024 + 1)
        after, named = os.fstat(descriptor), path.lstat()
        keys = (
            "st_dev",
            "st_ino",
            "st_uid",
            "st_mode",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        if (
            len(raw) != before.st_size
            or any(getattr(before, k) != getattr(after, k) for k in keys)
            or any(getattr(after, k) != getattr(named, k) for k in keys)
        ):
            raise ValueError("registration changed during observation")
        return raw
    finally:
        os.close(descriptor)


def _link(path):
    info = path.lstat()
    if not stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
        raise ValueError("registration is not its owned symlink")
    return {
        "device": info.st_dev,
        "inode": info.st_ino,
        "uid": info.st_uid,
        "target": os.readlink(path),
    }


def object_identity(path, *, directory=False):
    path = Path(path)
    if not directory and path.is_symlink():
        return _link(path)
    if path.resolve(strict=True) != path:
        raise ValueError("owned registration has a linked ancestor")
    if not directory:
        _read(path)
    info = path.lstat()
    if (
        info.st_uid != os.getuid()
        or info.st_mode & 0o022
        or (directory and not stat.S_ISDIR(info.st_mode))
    ):
        raise ValueError("owned registration parent or object changed ownership")
    return {
        "device": info.st_dev,
        "inode": info.st_ino,
        "uid": info.st_uid,
        "mode": stat.S_IMODE(info.st_mode),
    }


def registration(item):
    """Pin the preflight object and parent before creating any new generation."""
    path = Path(item["path"])
    return {
        **item,
        "before_identity": object_identity(path),
        "parent_identity": object_identity(path.parent, directory=True),
    }


def stage(item):
    """Create exact forward/reverse generations; the common receipt seals them."""
    path = Path(item["path"])
    if (
        object_identity(path) != item["before_identity"]
        or object_identity(path.parent, directory=True) != item["parent_identity"]
        or _state(item) not in {"before", "same"}
    ):
        raise ValueError("registration changed before sealed generation preparation")
    result = dict(item)
    for key, target in (("forward", "after"), ("reverse", "before")):
        descriptor, raw = tempfile.mkstemp(prefix=".runtime-location-", dir=path.parent)
        staged = Path(raw)
        try:
            if item["type"] == "link":
                os.close(descriptor)
                staged.unlink()
                staged.symlink_to(item["before"]["target"] if target == "before" else item["after"])
            else:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(base64.b64decode(item[target]))
                    stream.flush()
                    os.fchmod(stream.fileno(), item["mode"])
                    os.fsync(stream.fileno())
            parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(parent)
            finally:
                os.close(parent)
            result[key] = {"path": str(staged), "identity": object_identity(staged)}
        except BaseException:
            staged.unlink(missing_ok=True)
            raise
    if object_identity(path) != item["before_identity"]:
        raise ValueError("registration changed while its generations were prepared")
    return result


def _wrapper(root, owner, name):
    entry = "devflow-delivery" if name == "devflow" else name
    return (
        "#!/bin/sh\n# Canonical delivery entry; devflow aliases devflow-delivery.\nexec "
        + shlex.quote(str(root / "runtime/.venv/bin" / entry))
        + " --config "
        + shlex.quote(str(owner))
        + ' "$@"\n'
    ).encode()


def _files(root):
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_dir() and not path.is_symlink():
            continue
        if len(result) >= 64:
            raise ValueError("owned plugin exceeds its bounded content inventory")
        result[str(path.relative_to(root))] = base64.b64encode(_read(path)).decode()
    return result


def _cli(supplied):
    from .delivery_runtime_installation import _hash

    path = Path(supplied["host_cli"])
    if _hash(path) != supplied["host_cli_sha256"] or not os.access(path, os.X_OK):
        raise ValueError("public host plugin CLI changed")
    return str(path)


def update_leases(supplied, leases):
    """Use the installed owners' same locks; keep ProjectSync outside them."""
    home = Path(supplied["host_home"])
    if home != Path.home().resolve(strict=True):
        raise ValueError("installer leases belong to a different host home")
    link = _link(home / ".local/bin/devflow")
    target = Path(link["target"])
    journal = target.parent.parent
    if target.parent.name != "entries" or not journal.is_relative_to(
        home / ".local/share/devflow/delivery-launchers"
    ):
        raise ValueError("launcher lease belongs to another registration")
    source = source_root() / "scripts/install-delivery-launchers.py"
    spec = importlib.util.spec_from_file_location("devflow_installed_launcher_owner", source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    leases.enter_context(module.locked(journal))
    owned, _package = _desktop()
    leases.enter_context(owned.locked(home / ".codex"))


def observe(config, supplied, original, installed):
    home = Path(supplied["host_home"])
    if home != Path.home().resolve(strict=True) or home.resolve(strict=True) != home:
        raise ValueError("installation host is not the current owning user home")
    old_root = Path(original["policy"]["codex_bin"]).parents[7]
    root = Path(installed["root"])
    objects = []
    launcher = None
    for name in ("devflow", "devflow-delivery", "devflow-delivery-mcp"):
        link = home / ".local/bin" / name
        registered = _link(link)
        target = Path(registered["target"])
        if (
            not target.is_relative_to(home / ".local/share/devflow/delivery-launchers")
            or target.parent.name != "entries"
            or target.name != name
        ):
            raise ValueError("global delivery command has a foreign registration")
        before = _read(target)
        if before != _wrapper(old_root, config.path, name):
            raise ValueError("global delivery wrapper differs from the original owner/program")
        marker_path = target.parent.parent / "activation-manifest.json"
        raw_marker = _read(marker_path)
        marker = json.loads(raw_marker)
        if (
            marker.get("config", {}).get("config_path") != str(config.path)
            or marker.get("entries", {}).get(name, {}).get("path") != str(target)
            or marker["entries"][name].get("sha256") != hashlib.sha256(before).hexdigest()
        ):
            raise ValueError("original launcher activation does not own this exact registration")
        current_marker = {
            "path": str(marker_path),
            "sha256": hashlib.sha256(raw_marker).hexdigest(),
            "identity": object_identity(marker_path),
        }
        if launcher is not None and launcher != current_marker:
            raise ValueError("delivery commands have different original activation owners")
        launcher = current_marker
        objects.append(
            {
                "path": str(target),
                "type": "file",
                "mode": 0o755,
                "before": base64.b64encode(before).decode(),
                "after": base64.b64encode(_wrapper(root, config.path, name)).decode(),
                "public_link": str(link),
                "public_link_identity": registered,
            }
        )
    logical = config.helpers_dir.parent
    parent = logical.resolve(strict=True)
    helper = parent / "scripts"
    before = _link(helper)
    if before["target"] != str(old_root / "skills/devflow/scripts"):
        raise ValueError("configured helper is not the exact original scripts registration")
    reference = parent / "references"
    reference_identity = _link(reference)
    if any(
        value in reference_identity["target"]
        for value in ("/.codex/worktrees/", "/.codex-workspaces/")
    ):
        raise ValueError("retained references are not permanent; a different migration is required")
    objects.append(
        {
            "path": str(helper),
            "type": "link",
            "before": before,
            "after": str(root / "skills/devflow/scripts"),
        }
    )
    # Existing delivery supervisors must use their public lifecycle before this
    # operation. Never edit a loaded plist, and never touch ProjectSync agents.
    for path in (home / "Library/LaunchAgents").glob("com.ebarti.devflow.delivery.*.plist"):
        value = plistlib.loads(_read(path))
        if value.get("DevflowConfig") == str(config.path):
            raise ValueError("uninstall this delivery launchd agent publicly before relocation")
    owned, package = _desktop()
    codex = _cli(supplied)
    codex_home = home / ".codex"
    snapshot = owned.snapshot(codex, codex_home)
    if snapshot["direct"] is not None or not snapshot["plugin"] or not snapshot["marketplace"]:
        raise ValueError("location migration requires the inspected existing primary plugin")
    cache = codex_home / "plugins/cache/devflow-local/devflow/0.1.0"
    before_files = _files(cache)
    after_files = {
        name: base64.b64encode(raw).decode()
        for name, raw in package.package_files(root / "runtime", config.path, "codex").items()
    }
    expected_old = dict(after_files)
    old_mcp = json.loads(base64.b64decode(expected_old[".mcp.json"]))
    old_mcp["mcpServers"]["devflow-local-delivery"]["command"] = str(
        old_root / "runtime/.venv/bin/devflow-delivery-mcp"
    )
    expected_old[".mcp.json"] = base64.b64encode(package.json_bytes(old_mcp)).decode()
    if before_files != expected_old:
        raise ValueError("installed plugin differs beyond its exact owned runtime location")
    owned.package_check(
        Path(snapshot["marketplace"]),
        {name: base64.b64decode(raw) for name, raw in before_files.items()},
    )
    return {
        "launcher_activation": launcher,
        "objects": objects,
        "logical_helper": str(logical),
        "logical_helper_identity": _link(logical) if logical.is_symlink() else None,
        "references": str(reference),
        "reference_identity": reference_identity,
        "plugin": {
            "before": snapshot,
            "before_files": before_files,
            "after_files": after_files,
            "cache": str(cache),
            "codex_home": str(codex_home),
            "cli": codex,
            "cli_sha256": supplied["host_cli_sha256"],
            "progress": str(
                config.state_root
                / "runtime-installations"
                / supplied["command_id"]
                / "plugin-progress.json"
            ),
            "new_marketplace": str(
                config.state_root
                / "runtime-installations"
                / supplied["command_id"]
                / "plugin-package"
            ),
        },
    }


def _state(item):
    path = Path(item["path"])
    if (
        item.get("parent_identity")
        and object_identity(path.parent, directory=True) != item["parent_identity"]
    ):
        raise ValueError("registration parent generation changed")
    if item.get("before_identity"):
        current_identity = object_identity(path)
        expected = item["before_identity"]
        states = {"before": [expected]}
        if item.get("forward"):
            states["after"] = [item["forward"]["identity"]]
            states["before"].append(item["reverse"]["identity"])
        allowed = next(
            (state for state, values in states.items() if current_identity in values), None
        )
        if allowed is None:
            raise ValueError("registration object is not an acknowledged owned generation")
    else:
        allowed = None
    if item["type"] == "link":
        current = _link(path)
        if item["before"]["target"] == item["after"] == current["target"]:
            return "same"
        if (allowed in {None, "before"}) and (current["target"] == item["before"]["target"]):
            return "before"
        if allowed in {None, "after"} and current["target"] == item["after"]:
            return "after"
    else:
        current = base64.b64encode(_read(path)).decode()
        if item["before"] == item["after"] == current:
            return "same"
        if allowed in {None, "before"} and current == item["before"]:
            return "before"
        if allowed in {None, "after"} and current == item["after"]:
            return "after"
    raise ValueError("owned registration drifted; preserving foreign bytes")


def sealed_objects(receipt):
    values = receipt["installed_objects"]
    expected = receipt["registrations"]["objects"]
    if not isinstance(values, list) or len(values) != len(expected):
        raise ValueError("installed object generations have no exact preflight authority")
    names = set()
    for item, original in zip(values, expected, strict=True):
        if {
            key: value for key, value in item.items() if key not in {"forward", "reverse"}
        } != original:
            raise ValueError("sealed generation changed its original registration")
        path = Path(item["path"])
        current = object_identity(path)
        original_identity = item["before_identity"]
        forward, reverse = item["forward"], item["reverse"]
        positions = (
            (original_identity, forward["identity"], reverse["identity"]),
            (forward["identity"], original_identity, reverse["identity"]),
            (reverse["identity"], original_identity, forward["identity"]),
        )
        position = next((value for value in positions if value[0] == current), None)
        if position is None:
            raise ValueError("installed registration acquired another object generation")
        for stage_value, identity in zip((forward, reverse), position[1:], strict=True):
            staged = Path(stage_value["path"])
            if (
                staged in names
                or staged.parent != path.parent
                or staged == path
                or not staged.name.startswith(".runtime-location-")
                or object_identity(staged) != identity
            ):
                raise ValueError("sealed installer staging name or object generation changed")
            names.add(staged)
            after = identity == forward["identity"]
            content = item["after"] if after else item["before"]
            if item["type"] == "link":
                wanted = content if after else content["target"]
                if os.readlink(staged) != wanted:
                    raise ValueError("sealed helper staging target changed")
            elif base64.b64encode(_read(staged)).decode() != content:
                raise ValueError("sealed registration staging bytes changed")
        _state(item)
    return values


def _exchange(left, right):
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        native = libc.renamex_np
        native.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
        result = native(os.fsencode(left), os.fsencode(right), 0x00000002)
    elif hasattr(libc, "renameat2"):
        native = libc.renameat2
        native.argtypes = (
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_int,
            ctypes.c_char_p,
            ctypes.c_uint,
        )
        result = native(-100, os.fsencode(left), -100, os.fsencode(right), 2)
    else:
        raise ValueError("atomic registration exchange is unavailable")
    if result:
        raise OSError(ctypes.get_errno(), "atomic registration exchange failed")


def replace(item, *, reverse=False):
    """Exchange and authenticate the displaced object, restoring a raced foreign name."""
    wanted = "before" if reverse else "after"
    current = _state(item)
    if current in {wanted, "same"}:
        return
    path = Path(item["path"])
    original_identity = path.lstat()
    if "forward" in item:
        sealed = item["reverse" if reverse else "forward"]
        stage_path = Path(sealed["path"])
        if (
            stage_path.parent != path.parent
            or not stage_path.name.startswith(".runtime-location-")
            or object_identity(stage_path) != sealed["identity"]
        ):
            raise ValueError("sealed installer generation was lost or replaced")
        target = item["before"]["target"] if reverse and item["type"] == "link" else item[wanted]
        if item["type"] == "link":
            if os.readlink(stage_path) != target:
                raise ValueError("sealed helper generation target changed")
        elif base64.b64encode(_read(stage_path)).decode() != target:
            raise ValueError("sealed installer generation bytes changed")
        _exchange(stage_path, path)
        displaced = stage_path.lstat()
        if (displaced.st_dev, displaced.st_ino, displaced.st_uid) != (
            original_identity.st_dev,
            original_identity.st_ino,
            original_identity.st_uid,
        ):
            if object_identity(path) == sealed["identity"]:
                _exchange(stage_path, path)
            raise ValueError("sealed registration exchange raced a foreign object")
        displaced_ok = (
            os.readlink(stage_path)
            == (item["before"]["target"] if current == "before" else item["after"])
            if item["type"] == "link"
            else base64.b64encode(_read(stage_path)).decode() == item[current]
        )
        if not displaced_ok:
            if object_identity(path) == sealed["identity"]:
                _exchange(stage_path, path)
            raise ValueError("displaced sealed registration bytes changed; preserving them")
        # The exact displaced object remains at its sealed staging name. Keeping
        # it makes both the crash window and rollback independently attributable.
        if _state(item) != wanted:
            raise ValueError("sealed installed generation changed after publication")
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return
    descriptor, name = tempfile.mkstemp(prefix=".runtime-location-", dir=path.parent)
    stage = Path(name)
    os.close(descriptor)
    safe_unlink = True
    try:
        if item["type"] == "link":
            stage.unlink()
            stage.symlink_to(item["before"]["target"] if reverse else item["after"])
        else:
            with stage.open("wb") as stream:
                stream.write(base64.b64decode(item[wanted]))
                stream.flush()
                os.fchmod(stream.fileno(), item["mode"])
                os.fsync(stream.fileno())
        if _state(item) != current:
            raise ValueError("registration changed before its guarded exchange")
        _exchange(stage, path)
        safe_unlink = False
        displaced = stage.lstat()
        if (displaced.st_dev, displaced.st_ino, displaced.st_uid) != (
            original_identity.st_dev,
            original_identity.st_ino,
            original_identity.st_uid,
        ):
            # Restore only if our published object still occupies the name.
            if _state(item) == wanted:
                _exchange(stage, path)
                safe_unlink = True
            raise ValueError("registration raced its exchange; foreign object preserved")
        if item["type"] == "file":
            if base64.b64encode(_read(stage)).decode() != item[current]:
                if _state(item) == wanted:
                    _exchange(stage, path)
                    safe_unlink = True
                raise ValueError("displaced registration bytes changed during exchange")
        elif os.readlink(stage) != (
            item["before"]["target"] if current == "before" else item["after"]
        ):
            if _state(item) == wanted:
                _exchange(stage, path)
                safe_unlink = True
            raise ValueError("displaced helper target changed during exchange")
        safe_unlink = True  # Exact own beforeimage is retained in the immutable receipt.
        if _state(item) != wanted:
            raise ValueError("registration changed after its exchange")
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if safe_unlink:
            stage.unlink(missing_ok=True)


def _plugin_state(plan):
    from .delivery_runtime_installation import _hash

    owned, _package = _desktop()
    owned.package_check(
        Path(plan["before"]["marketplace"]),
        {name: base64.b64decode(raw) for name, raw in plan["before_files"].items()},
    )
    cli = plan["cli"]
    if _hash(Path(cli)) != plan["cli_sha256"]:
        raise ValueError("public host CLI changed during installation")
    observed = owned.snapshot(cli, Path(plan["codex_home"]), plan["before"]["unrelated_sha256"])
    if observed["unrelated_sha256"] != plan["before"]["unrelated_sha256"] or observed["direct"]:
        raise ValueError("unrelated host state changed; installation preserves it")
    market = observed["marketplace"]
    if market not in (None, plan["before"]["marketplace"], plan["new_marketplace"]):
        raise ValueError("owned plugin marketplace was replaced")
    payload = None
    if observed["plugin"]:
        payload = _files(Path(plan["cache"]))
        if payload not in (plan["before_files"], plan["after_files"]):
            raise ValueError("owned plugin cache changed beyond the acknowledged location")
    return observed, payload


def plugin_apply(plan, guard, *, reverse=False):
    """Observe uncertain public outcomes before deciding the next exact effect."""
    owned, _package = _desktop()
    target_market = plan["before"]["marketplace"] if reverse else plan["new_marketplace"]
    target_files = plan["before_files"] if reverse else plan["after_files"]
    from .delivery_resources import read_private, write_private

    progress = Path(plan["progress"])
    log = read_private(progress) if progress.exists() else {"attempts": [], "observations": []}
    for _ in range(5):
        guard()
        observed, payload = _plugin_state(plan)
        if (
            observed["plugin"]
            and payload == target_files
            and observed["marketplace"] == target_market
        ):
            return
        before = digest([observed, payload])
        last = log["attempts"][-1] if log["attempts"] else None
        if last and last["before_sha256"] == before and last.get("outcome") in {None, "unknown"}:
            raise ValueError("public plugin effect is still unknown; no automatic replay")
        if observed["plugin"]:
            argv = ("plugin", "remove", "devflow@devflow-local", "--json")
        elif observed["marketplace"] is not None and observed["marketplace"] != target_market:
            argv = ("plugin", "marketplace", "remove", "devflow-local")
        elif observed["marketplace"] is None:
            argv = ("plugin", "marketplace", "add", target_market)
        else:
            argv = ("plugin", "add", "devflow@devflow-local", "--json")
        attempt = {"argv": list(argv), "before_sha256": before}
        log["attempts"].append(attempt)
        write_private(progress, log)
        failed = False
        try:
            owned.command(plan["cli"], Path(plan["codex_home"]), *argv)
        except (ValueError, OSError):
            failed = True
        after, new_payload = _plugin_state(plan)
        changed = digest([after, new_payload]) != before
        attempt["outcome"] = "unknown" if failed or not changed else "acknowledged"
        log["observations"].append(
            {"attempt": len(log["attempts"]), "state_sha256": digest([after, new_payload])}
        )
        write_private(progress, log)
        if not changed:
            raise ValueError("public plugin effect outcome is unknown; inspect before replay")
        guard()
    raise ValueError("public plugin migration did not reach its exact observed target")


def package(plan):
    _owned, module = _desktop()
    root = Path(plan["new_marketplace"])
    files = {name: base64.b64decode(raw) for name, raw in plan["after_files"].items()}
    from .delivery_filesystem_custody import _immutable

    catalog = {
        "name": "devflow-local",
        "interface": {"displayName": "Devflow Local"},
        "plugins": [module.ENTRY],
    }
    for name, raw in files.items():
        _immutable(root / "plugins/devflow" / name, raw)
    _immutable(root / ".agents/plugins/marketplace.json", module.json_bytes(catalog))


def validate(plan, *, after=False, before=False, objects=None):
    activation = plan["launcher_activation"]
    if (
        object_identity(Path(activation["path"])) != activation["identity"]
        or hashlib.sha256(_read(Path(activation["path"]))).hexdigest() != activation["sha256"]
    ):
        raise ValueError("original launcher activation bytes or generation changed")
    logical = Path(plan["logical_helper"])
    if (
        plan["logical_helper_identity"] is not None
        and _link(logical) != plan["logical_helper_identity"]
    ):
        raise ValueError("logical compatibility registration changed")
    if _link(Path(plan["references"])) != plan["reference_identity"]:
        raise ValueError("already durable references registration changed")
    for item in plan["objects"] if objects is None else objects:
        if (
            item.get("public_link")
            and _link(Path(item["public_link"])) != item["public_link_identity"]
        ):
            raise ValueError("global command public link changed")
        current = _state(item)
        if after and current not in {"after", "same"}:
            raise ValueError("owned program registration is not completely migrated")
        if before and current not in {"before", "same"}:
            raise ValueError("owned program registration is not at its original beforeimage")
    observed, payload = _plugin_state(plan["plugin"])
    if after and (
        not observed["plugin"]
        or payload != plan["plugin"]["after_files"]
        or observed["marketplace"] != plan["plugin"]["new_marketplace"]
    ):
        raise ValueError("public plugin registration is not completely migrated")
    if before and (
        not observed["plugin"]
        or payload != plan["plugin"]["before_files"]
        or observed["marketplace"] != plan["plugin"]["before"]["marketplace"]
    ):
        raise ValueError("public plugin registration is not at its original beforeimage")
