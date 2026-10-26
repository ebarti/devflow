#!/usr/bin/env python3.12
"""Shared production-installation dependency checks (stdlib only).

Development registration is deliberately explicit and restricted to destinations
outside the user's production registration directories. It does not establish a
production installation proof.
"""

from __future__ import annotations

import json
import os
import sqlite3
import shlex
import shutil
import fcntl
import hashlib
import importlib.util
import re
import stat
import subprocess
import tempfile
from pathlib import Path

_REGISTRATION_LEASES = []
_CONTROLLER_FD = None
CONTROLLERS = {"devflow-delivery": "devflow_temporal.delivery_control",
               "devflow-delivery-mcp": "devflow_temporal.delivery_mcp",
               "devflow-temporal": "devflow_temporal.cli",
               "devflow-project-sync": "devflow_temporal.delivery_project_sync"}


def identity_helper():
    spec = importlib.util.spec_from_file_location("production_identity", Path(__file__).with_name("production-entry.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def node(path):
    return identity_helper().directory_identity(path)


def same_node(before, current):
    return identity_helper().same_identity(before, current)


def registration_lease(root):
    fd = os.open(root.parent / ".production-install.lock", os.O_RDWR | os.O_NOFOLLOW)
    try:
        fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        receipt = read_json(root.parent / ".production-install.json")
        if (receipt.get("managed_owner_version") != 1 or receipt.get("phase") != "verified"
                or not same_node(receipt.get("target_identity"), node(root))):
            raise ValueError("production installation is unfinished or requires authenticated legacy cutover")
        update = root.parent / ".production-update.json"
        if update.exists() and read_json(update).get("phase") not in {"verified", "rolled_back"}:
            raise ValueError("production update is unfinished; replay its retained recovery command")
        _REGISTRATION_LEASES.append(fd)
    except BaseException:
        os.close(fd)
        raise


def console_wrapper(root, controller, python, module, arguments=()):
    return ("#!/bin/sh\n# Devflow production ownership bootstrap.\n"
            "unset LD_PRELOAD LD_LIBRARY_PATH DYLD_INSERT_LIBRARIES DYLD_LIBRARY_PATH DYLD_FRAMEWORK_PATH\nexec "
            + shlex.quote(str(python)) + " -I -S " + shlex.quote(str(controller)) + " "
            + shlex.quote(str(root)) + " " + shlex.quote(module)
            + (" " + " ".join(shlex.quote(str(item)) for item in arguments) if arguments else "")
            + ' "$@"\n').encode()


def production_bootstrap(root, base_python, *, create=False, environment=None):
    revision = git(root, "rev-parse", "HEAD")
    folder = root.parent / ".production-bootstraps" / revision
    controller = folder / "production-entry.py"
    manifest = folder / "manifest.json"
    wanted = (git(root, "show", revision + ":scripts/production-entry.py") + "\n").encode()
    if create:
        protected_directory(folder, create=True)
        if not controller.exists():
            descriptor = os.open(controller, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(wanted)
                stream.flush()
                os.fsync(stream.fileno())
        data = {"revision": revision, "base_python": str(base_python), "runtime_dir": str(root / "runtime"),
                "controller_sha256": hashlib.sha256(wanted).hexdigest()}
        if manifest.exists() and read_json(manifest) != data:
            raise ValueError("immutable production bootstrap metadata changed")
        if not manifest.exists():
            write_json(manifest, data)
    data = read_json(manifest)
    durable(Path(data["base_python"]), "production bootstrap interpreter")
    if read_bytes(controller) != wanted or data["controller_sha256"] != hashlib.sha256(wanted).hexdigest():
        raise ValueError("production bootstrap differs from its independent Git object")
    env = environment or root / "runtime/.venv"
    if create:
        protected_directory(folder / "entries", create=True)
    for name, module in CONTROLLERS.items():
        entry = env / "bin" / name
        expected = console_wrapper(root, controller, data["base_python"], module)
        if create:
            descriptor, stage = tempfile.mkstemp(prefix=".devflow-controller-", dir=entry.parent)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(expected)
                stream.flush()
                os.fchmod(stream.fileno(), 0o755)
                os.fsync(stream.fileno())
            os.replace(stage, entry)
        if entry.read_bytes() != expected:
            raise ValueError("production console entry does not bind its independent bootstrap")
        published = folder / "entries" / name
        if create and not published.exists():
            descriptor = os.open(published, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o755)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(expected)
                stream.flush()
                os.fchmod(stream.fileno(), 0o755)
                os.fsync(stream.fileno())
        if published.is_symlink() or published.read_bytes() != expected:
            raise ValueError("immutable published controller entry changed")
        info = published.lstat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o755 or info.st_nlink != 1:
            raise ValueError("published controller entry is not an owned executable")
    if create:
        descriptor = os.open(env / "bin", os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        sync_tree(folder)


def published_entry(root, name):
    return (root.parent / ".production-bootstraps" / git(root, "rev-parse", "HEAD") / "entries" / name)


def development(*destinations):
    mode = os.environ.get("DEVFLOW_INSTALL_MODE", "production")
    if mode not in {"production", "development"}:
        raise ValueError("DEVFLOW_INSTALL_MODE must be production or development")
    if mode != "development":
        return False
    protected = [Path.home() / item for item in
                 (".agents", ".codex", ".local", "Library/LaunchAgents")]
    if not destinations:
        raise ValueError("development mode requires explicit isolated destinations")
    for value in destinations:
        path = Path(value).expanduser().resolve()
        if any(path == root or root in path.parents for root in protected):
            raise ValueError("development mode cannot replace production registrations")
    return True


def durable(value, label="installation dependency", *, allow_aliases=False):
    path = Path(value).expanduser()
    if not path.is_absolute() or ".." in path.parts or (not allow_aliases and path.resolve() != path):
        raise ValueError(f"{label} must be canonical and absolute: {path}")
    configured = json.loads(os.environ.get("DEVFLOW_MANAGED_WORKTREE_ROOTS", "[]"))
    if (not isinstance(configured, list) or any(not isinstance(item, str)
            or not Path(item).expanduser().is_absolute() for item in configured)):
        raise ValueError("DEVFLOW_MANAGED_WORKTREE_ROOTS must be a JSON array of absolute roots")
    for item in configured:
        roots = {Path(item).expanduser(), Path(item).expanduser().resolve()}
        if any(path == root or root in path.parents for root in roots):
            raise ValueError(f"{label} depends on a configured managed worktree root: {path}")
    temporary = {Path("/tmp"), Path("/private/tmp"), Path("/var/tmp"),
                 Path("/private/var/tmp"), Path("/var/folders"),
                 Path("/private/var/folders"), Path(tempfile.gettempdir()).resolve()}
    if (any(path == root or root in path.parents for root in temporary)
            or ".codex-workspaces" in path.parts
            or any(path.parts[i:i + 2] in {(".codex", "worktrees"),
                                          (".codex", "tmp"),
                                          (".codex", "archived_worktrees")}
                   for i in range(len(path.parts) - 1))):
        raise ValueError(f"{label} depends on a temporary or managed worktree: {path}")
    for parent in (path, *path.parents):
        if parent.exists() and parent.is_symlink():
            if not allow_aliases or parent.lstat().st_uid not in {0, os.getuid()}:
                raise ValueError(f"{label} contains an unowned or unsupported linked ancestor: {parent}")
        if allow_aliases and (parent.exists() or parent.is_symlink()):
            durable(parent.resolve(strict=True), label)
    return path


def protected_directory(path, *, create=False):
    durable(path)
    if create and not path.exists():
        if not path.parent.exists():
            protected_directory(path.parent, create=True)
        path.mkdir(mode=0o700)
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o022):
        raise ValueError("installation directory is not owned and protected: " + str(path))


def sync_tree(root):
    directories = [root]
    for item in root.rglob("*"):
        if item.is_symlink():
            continue
        if item.is_dir():
            directories.append(item)
        else:
            descriptor = os.open(item, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    for folder in reversed(directories):
        descriptor = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def git(root, *args):
    env = git_environment()
    return subprocess.check_output(["git", "-C", str(root), *args], text=True,
                                   stderr=subprocess.PIPE,
                                   env=env).strip()


def git_environment():
    # A clean .git directory is insufficient when ambient Git variables redirect
    # its object store, index or configuration to the disposable setup checkout.
    redirected = {"GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_OBJECT_DIRECTORY",
                  "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_INDEX_FILE", "GIT_NAMESPACE",
                  "GIT_CONFIG_PARAMETERS", "GIT_CONFIG_COUNT"}
    return {**{key: value for key, value in os.environ.items()
               if key not in redirected and not key.startswith(("GIT_CONFIG_KEY_", "GIT_CONFIG_VALUE_"))},
            "GIT_OPTIONAL_LOCKS": "0"}


def read_bytes(path):
    durable(path, "installation evidence")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
                or before.st_size > 4 * 1024 * 1024):
            raise ValueError("installation evidence is not a bounded private owned file")
        raw = os.read(fd, before.st_size + 1)
        after = os.fstat(fd)
        if (len(raw) != before.st_size or (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
            raise ValueError("installation evidence changed while reading")
        return raw
    finally:
        os.close(fd)


def read_json(path):
    return json.loads(read_bytes(path))


def inventory_path(root, receipt=None):
    receipt = receipt or read_json(root.parent / ".production-install.json")
    generation = receipt.get("owner_generation")
    if not isinstance(generation, str) or not re.fullmatch(r"[0-9a-f]{32}", generation):
        raise ValueError("production owner has no authenticated store-birth generation")
    return root.parent / ".production-configurations" / (generation + ".json")


def read_inventory(root, receipt=None):
    receipt = receipt or read_json(root.parent / ".production-install.json")
    value = read_json(inventory_path(root, receipt))
    if (value.get("schema_version") != 1 or value.get("birth_protocol") != 1
            or value.get("owner_generation") != receipt.get("owner_generation")
            or not same_node(value.get("target_identity"), node(root))):
        raise ValueError("production configuration birth inventory belongs to another owner")
    return value


def initialize_inventory(root, receipt):
    file = inventory_path(root, receipt)
    protected_directory(file.parent, create=True)
    if file.exists():
        read_inventory(root, receipt)
        return
    if not receipt.get("created") or not receipt.get("born_new_owner"):
        raise ValueError("production birth inventory is unavailable; its absence does not prove an unused owner")
    write_json(file, {"schema_version": 1, "birth_protocol": 1,
                     "owner_generation": receipt["owner_generation"],
                     "target_identity": receipt["target_identity"], "new_owner": True,
                     "configurations": {}, "store_observations": {}, "store_births": {}})


def write_json(path, value):
    import tempfile
    if path.exists():
        read_bytes(path)
    descriptor, name = tempfile.mkstemp(prefix=".production-evidence-", dir=path.parent)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(value, stream, sort_keys=True, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(name, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def require_update_inventory(root):
    receipt = read_json(root.parent / ".production-install.json")
    if receipt.get("managed_owner_version") != 1 or not same_node(receipt.get("target_identity"), node(root)):
        raise ValueError("legacy production owner has no authenticated complete configuration cutover; use supported migration")
    value = read_inventory(root, receipt)
    databases = {}
    for config, records in value["configurations"].items():
        persistence_path(Path(config), "registered configuration")
        for record in records:
            databases.setdefault(record["tracking_db"], set()).add(record["state_root"])
    for collection in (value["store_observations"], value["store_births"]):
        for database, record in collection.items():
            databases.setdefault(database, set()).update(record["state_roots"])
    for name, states in databases.items():
        database = persistence_path(Path(name), "registered tracking database")
        for state_root in states:
            state = persistence_path(Path(state_root), "registered persistence root")
            if any(os.path.lexists(state / "runtime-installations" / name)
                   for name in ("current.json", "pending.json")):
                raise ValueError("acknowledged installations require their public stopped runtime-installation transition: " + str(state))
        if not database.exists():
            observation = value["store_observations"].get(name)
            if (value.get("new_owner") is True and observation
                    and observation.get("first_observed_absent") is True
                    and name not in value["store_births"]):
                continue  # This born owner has never admitted a possible writer.
            raise ValueError("registered tracking database is unavailable; its absence does not prove no admitted work: " + str(database))
        with sqlite3.connect(database.as_uri() + "?mode=ro", uri=True) as db:
            for (raw,) in db.execute("SELECT request_json FROM delivery_runs"):
                spec = json.loads(raw)
                if (spec.get("provider") != "fake"
                        or spec.get("policy", {}).get("native_identity") is not None):
                    raise ValueError("frozen native authority requires its supported installation transition: " + str(database))


def source(root):
    root = durable(root, "runtime source")
    protected_directory(root)
    if ((root / ".git").is_symlink() or not (root / ".git").is_dir()
            or git(root, "rev-parse", "--show-toplevel") != str(root)
            or Path(git(root, "rev-parse", "--path-format=absolute", "--git-common-dir"))
            != root / ".git"):
        raise ValueError("production source must own its Git common directory")
    for name in ("alternates", "http-alternates"):
        if os.path.lexists(root / ".git/objects/info" / name):
            raise ValueError("production Git objects depend on an external object store")
    for item in (root / ".git").rglob("*"):
        if item.is_symlink():
            raise ValueError("production Git storage contains a linked dependency")
    if git(root, "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("production source has uncommitted or untracked files")
    for entry in git(root, "ls-files", "--stage").splitlines():
        if entry.split(" ", 1)[0] not in {"100644", "100755"}:
            raise ValueError("production source contains a linked file or external submodule")
    return {"revision": git(root, "rev-parse", "HEAD"),
            "tree": git(root, "rev-parse", "HEAD^{tree}")}


# Run inside the destination interpreter, without inherited PYTHONPATH/user site.
PROBE = r'''
import importlib, importlib.metadata, json, pathlib, sys, tomllib
root = pathlib.Path(sys.argv[1]); env = pathlib.Path(sys.argv[2])
assert pathlib.Path(sys.prefix) == env, "interpreter prefix left the owned environment"
result = {"base_prefix": sys.base_prefix, "base_executable": sys._base_executable,
          "imports": {}, "paths": sys.path}
for name in ("devflow_temporal.delivery_control", "devflow_temporal.delivery_mcp",
             "devflow_temporal.delivery_api", "temporalio", "mcp", "fastapi"):
    module = importlib.import_module(name)
    result["imports"][name] = str(pathlib.Path(module.__file__).resolve())
result["direct_urls"] = []
for distribution in importlib.metadata.distributions():
    text = distribution.read_text("direct_url.json")
    if text:
        result["direct_urls"].append(json.loads(text))
from devflow_temporal.delivery_runtime_installation import _records
site = env / ("lib/python%d.%d/site-packages" % sys.version_info[:2])
packages = _records(site)
locked = tomllib.loads((root / "runtime/uv.lock").read_text())["package"]
for name, item in packages.items():
    assert any(package["name"].replace("_", "-") == name and
               package["version"] == item["version"] for package in locked), \
           "installed dependency differs from frozen lock: " + name
binary = pathlib.Path(importlib.metadata.distribution("openai-codex-cli-bin").locate_file(
    "codex_cli_bin/bin/codex"))
assert binary == site / "codex_cli_bin/bin/codex" and binary.resolve() == binary, \
       "bundled CLI left the owned environment"
print(json.dumps(result))
'''


def runtime(root, *, environment=None, allow_unpublished=False):
    if root != Path.home().resolve() / ".local/share/devflow/developer-workflow":
        raise ValueError("production runtime must use its independently owned fixed installation root")
    identity = source(root)
    env = environment or root / "runtime/.venv"
    protected_directory(env)
    for item in env.rglob("*"):
        if item.is_symlink():
            # venv's Python executables may use a separately durable base Python.
            if item.parent != env / "bin" or not item.name.startswith("python"):
                raise ValueError("runtime environment contains an external linked dependency")
            durable(item.resolve(strict=True), "base Python")
    python = env / "bin/python"
    result = subprocess.run([str(python), "-I", "-B", "-c", PROBE, str(root), str(env)],
                            env={key: value for key, value in os.environ.items()
                                 if not key.startswith("PYTHON") and key not in {
                                     "VIRTUAL_ENV", "__PYVENV_LAUNCHER__", "LD_PRELOAD", "LD_LIBRARY_PATH",
                                     "DYLD_INSERT_LIBRARIES", "DYLD_LIBRARY_PATH", "DYLD_FRAMEWORK_PATH"}},
                            text=True, capture_output=True, timeout=90,
                            pass_fds=tuple(_REGISTRATION_LEASES) +
                            (() if _CONTROLLER_FD is None else (_CONTROLLER_FD,)))
    if result.returncode:
        raise ValueError("owned runtime import proof failed: " + result.stderr.strip())
    observed = json.loads(result.stdout)
    durable(observed["base_prefix"], "base Python library")
    for name, value in observed["imports"].items():
        path = durable(value, "runtime import")
        expected = root / "runtime/src" if name.startswith("devflow_temporal.") else env
        if expected not in path.parents:
            raise ValueError("runtime import left its owned source/environment: " + name)
    for value in observed["paths"]:
        if value:
            # uv's stable base-Python aliases may appear in sys.path; the
            # interpreter/environment proof still pins the actual resolved
            # target. Registrations and state keep their stricter no-alias rule.
            durable(Path(value).resolve(), "Python import search path")
    for line in (env / "pyvenv.cfg").read_text().splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() in {"home", "executable", "base-executable", "base-prefix"}:
            durable(Path(value.strip()).resolve(), "virtual environment base")
    from urllib.parse import unquote, urlparse
    for record in observed["direct_urls"]:
        url = urlparse(record.get("url", ""))
        if url.scheme == "file":
            target = durable(unquote(url.path), "editable/local package")
            if target != root / "runtime":
                raise ValueError("runtime depends on another editable/local project")
    for pth in env.glob("lib/python*/site-packages/*.pth"):
        for line in pth.read_text().splitlines():
            if line.strip() and not line.startswith(("#", "import ", "import\t")):
                target = (pth.parent / line).resolve()
                durable(target, "Python .pth dependency")
                if env not in target.parents and target != root / "runtime/src":
                    raise ValueError("Python .pth dependency left the owned environment")
    wrapped = False
    for name in ("devflow-delivery", "devflow-delivery-mcp"):
        entry = env / "bin" / name
        text = entry.read_text()
        if text.startswith("#!/bin/sh\n# Devflow production ownership bootstrap."):
            wrapped = True
            continue
        expected_python = root / "runtime/.venv/bin/python"
        if not allow_unpublished or str(expected_python) not in "\n".join(text.splitlines()[0:4]):
            raise ValueError("runtime launcher does not use the owned interpreter")
    if wrapped and not allow_unpublished:
        production_bootstrap(root, observed["base_executable"], environment=env)
    if source(root) != identity:
        raise ValueError("production source changed during runtime verification")
    return {**identity, "runtime": str(root / "runtime"), "imports": observed["imports"],
            "base_python": str(durable(Path(observed["base_executable"]).resolve(strict=True),
                                       "recovery base interpreter"))}


def persistence_path(path, label):
    path = durable(path, label)
    source_root = Path.home().resolve() / ".local/share/devflow/developer-workflow"
    mutable = [source_root, *(source_root.parent / name for name in
               (".production-environments", ".production-recovery", ".production-bootstraps",
                ".production-configurations"))]
    if any(path == root or root in path.parents or path in root.parents for root in mutable):
        raise ValueError("production persistence must be separate from installation/update dependencies: " + label)
    return path


def configuration(path):
    path = durable(path, "service configuration")
    value = json.loads(path.read_text())
    for name in ("state_root", "tracking_db", "helpers_dir", "codex_bin"):
        if name not in value:
            raise ValueError("production service configuration must name " + name)
        durable(value[name], "service " + name, allow_aliases=name == "helpers_dir")
    for name, dependency in (("configuration", path), ("state_root", Path(value["state_root"])),
                             ("tracking_db", Path(value["tracking_db"]))):
        persistence_path(dependency, name)
    registry = Path(value.get("execution_registry", str(
        Path.home() / ".local/state/devflow/execution-ownership/registry.sqlite3")))
    persistence_path(registry, "execution registry")
    helpers = Path(value["helpers_dir"]).resolve(strict=True)
    protected_directory(helpers)
    if not helpers.is_dir() or any(item.is_symlink() for item in helpers.rglob("*")):
        raise ValueError("production helper inventory is missing or contains linked dependencies")
    execution_dependency(Path(value["codex_bin"]), os.environ.get("PATH", ""))
    for name in ("temporal_bin",):
        if value.get(name):
            durable(value[name], "service " + name)
            execution_dependency(Path(value[name]), os.environ.get("PATH", ""))
    return value


def execution_dependency(path, search, seen=None):
    """Check executable interpreters and explicit wrapper handoffs recursively."""
    seen = set() if seen is None else seen
    path = durable(Path(path).resolve(strict=True), "executable dependency")
    if path in seen:
        return
    seen.add(path)
    if not path.is_file() or not os.access(path, os.X_OK):
        raise ValueError("executable dependency is not executable: " + str(path))
    with path.open("rb") as stream:
        head = stream.read(16384)
    if not head.startswith(b"#!"):
        return
    lines = head.decode(errors="replace").splitlines()
    words = shlex.split(lines[0][2:])
    if not words:
        raise ValueError("executable has an empty interpreter binding")
    interpreter = Path(words[0]).resolve(strict=True)
    execution_dependency(interpreter, search, seen)
    if interpreter.name == "env":
        arguments = [word for word in words[1:] if word != "-S"]
        if not arguments or arguments[0].startswith("-"):
            raise ValueError("executable has an unresolved env interpreter")
        resolved = shutil.which(arguments[0], path=search)
        if not resolved:
            raise ValueError("executable's env interpreter is unavailable")
        execution_dependency(Path(resolved), search, seen)
    for line in lines[1:]:
        if line.strip().startswith("exec "):
            handoff = shlex.split(line.strip()[5:])
            if not handoff or "$" in handoff[0] or "`" in handoff[0]:
                raise ValueError("executable wrapper has an unresolved execution handoff")
            resolved = handoff[0] if Path(handoff[0]).is_absolute() else shutil.which(handoff[0], path=search)
            if not resolved:
                raise ValueError("executable wrapper handoff is unavailable")
            execution_dependency(Path(resolved), search, seen)
