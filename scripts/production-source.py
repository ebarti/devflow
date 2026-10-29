#!/usr/bin/env python3.12
"""Qualified source startup under the installation's actual lifetime lease.

This is a supported-loader contract. The updater never edits a generation and
needs EX, while a qualified process retains SH. Checks detect ordinary drift;
advisory locks and hashes do not exclude arbitrary same-user transient writes,
monkeypatching or replacement of the trusted interpreter/site/bootstrap itself.
"""

from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

SCHEMA = "devflow.source-start/v1"
FD_ENV = "DEVFLOW_SOURCE_START_FD"
CACHE = "/dev/null"
LIMIT = 4 * 1024 * 1024
SCOPES = ("runtime/src/devflow_temporal", "skills/devflow/scripts",
          "scripts/installation-paths.py", "scripts/production-layout.py", "scripts/production-entry.py")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def stamp(info):
    return [getattr(info, key) for key in ("st_dev", "st_ino", "st_uid", "st_mode", "st_nlink",
                                         "st_size", "st_mtime_ns", "st_ctime_ns")]


def custody_failure_diagnostic(info, private, owned, role):
    if (os.environ.get("DEVFLOW_CI_FAILURE_DIAGNOSTICS") != "runner-custody-v1"
            or os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("GITHUB_REPOSITORY") != "ebarti/devflow"):
        return
    try:
        roles = {"site_registration", "site_support", "base_interpreter", "git_observer",
                 "venv_configuration", "bootstrap_metadata", "source_manifest",
                 "bootstrap_helper", "tracked_source", "registered_site"}
        facts = {"role": role if role in roles else "unspecified",
                 "regular_file": stat.S_ISREG(info.st_mode), "owner_required": bool(owned),
                 "owner_matches": info.st_uid == os.getuid() if owned else None,
                 "group_or_other_writable": bool(info.st_mode & 0o022),
                 "private_required": bool(private),
                 "private_mode_matches": stat.S_IMODE(info.st_mode) == 0o600 if private else None,
                 "private_single_link": info.st_nlink == 1 if private else None,
                 "within_size_limit": info.st_size <= 64 * 1024 * 1024}
        print(json.dumps({"source_custody_refusal": facts}, sort_keys=True), file=sys.stderr)
    except BaseException:
        pass


def opened(path, *, private=False, owned=True, role="unspecified"):
    path = Path(path)
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("qualified source contains an alias")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or (owned and before.st_uid != os.getuid())
                or before.st_mode & 0o022 or (private and (
                    stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1))
                or before.st_size > 64 * 1024 * 1024):
            custody_failure_diagnostic(before, private, owned, role)
            raise ValueError("qualified source file lost its protected custody")
        with os.fdopen(os.dup(descriptor), "rb") as stream:
            raw = stream.read(before.st_size + 1)
        if (len(raw) != before.st_size or stamp(before) != stamp(os.fstat(descriptor))
                or stamp(before) != stamp(path.lstat())):
            raise ValueError("qualified source file changed while measured")
        return raw, stamp(before)
    finally:
        os.close(descriptor)


def private(path, *, role="unspecified"):
    raw, _stamp = opened(path, private=True, role=role)
    if len(raw) > LIMIT:
        raise ValueError("qualified source evidence exceeds its bound")
    return json.loads(raw), sha(raw)


def git(binary, root, *arguments):
    environment = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    environment.update(GIT_OPTIONAL_LOCKS="0", GIT_NO_REPLACE_OBJECTS="1", GIT_GRAFT_FILE=os.devnull)
    return subprocess.check_output([str(binary), "--no-replace-objects", "-c", "core.fsmonitor=false",
                                    "-c", "core.hooksPath=/dev/null", "-C", str(root), *arguments],
                                   env=environment, stderr=subprocess.PIPE, timeout=30)


def build(root, base_python, git_bin, revision, tree):
    root, base_python, git_bin = map(Path, (root, base_python, git_bin))
    source = {}
    for entry in git(git_bin, root, "ls-tree", "-rz", revision, "--", *SCOPES).split(b"\0"):
        if not entry:
            continue
        metadata, name = entry.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        relative = name.decode()
        if mode not in {"100644", "100755"} or kind != "blob":
            raise ValueError("qualified source manifest contains a nonregular Git object")
        source[relative] = sha(git(git_bin, root, "cat-file", "blob", oid))
    if not source or len(source) > 2048:
        raise ValueError("qualified source manifest is empty or excessive")
    site_paths = list((root / "runtime/.venv/lib").glob("python*/site-packages"))
    if len(site_paths) != 1:
        raise ValueError("qualified source requires one owned site-packages directory")
    site = site_paths[0]
    site_files = {str(file.relative_to(root)): sha(opened(file, role="site_registration")[0])
                  for file in sorted(site.glob("*.pth"))}
    for name in ("_virtualenv.py",):
        if (site / name).exists():
            site_files[str((site / name).relative_to(root))] = sha(opened(site / name, role="site_support")[0])
    base_raw, _base_stamp = opened(base_python, owned=False, role="base_interpreter")
    git_raw, _git_stamp = opened(git_bin, owned=False, role="git_observer")
    return {"schema": SCHEMA, "root": str(root), "revision": revision, "tree": tree,
            "source": source, "site": site_files, "site_root": str(site.relative_to(root)),
            "venv_config_sha256": sha(opened(root / "runtime/.venv/pyvenv.cfg", role="venv_configuration")[0]),
            "base_python": str(base_python), "base_python_sha256": sha(base_raw),
            "git_bin": str(git_bin), "git_sha256": sha(git_raw)}


def layout():
    spec = importlib.util.spec_from_file_location("source_layout", Path(__file__).with_name("production-layout.py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def active(root):
    value = layout().installed(root)
    if value is None or value["version"] != 2:
        raise ValueError("qualified source startup requires an active immutable generation")
    startup = value["birth"].get("startup")
    folder = value["owner"] / ".production-bootstraps" / (value["birth"]["revision"] + "-" + value["generation_id"])
    if not startup or startup.get("directory") != str(folder):
        raise ValueError("generation has no qualified startup manifest; prepare a supported successor")
    metadata, metadata_sha = private(folder / "manifest.json", role="bootstrap_metadata")
    manifest, manifest_sha = private(folder / "source.json", role="source_manifest")
    if (metadata_sha != startup["manifest_sha256"] or manifest_sha != metadata["source_manifest_sha256"]
            or sha(opened(folder / "production-source.py", private=True, role="bootstrap_helper")[0]) != metadata["source_helper_sha256"]
            or manifest.get("schema") != SCHEMA or manifest.get("root") != str(root)
            or manifest.get("revision") != value["birth"]["revision"]
            or manifest.get("tree") != value["birth"]["tree"]):
        raise ValueError("qualified source startup manifest differs from its birth")
    return value, folder, manifest, manifest_sha


def importable(root):
    package = root / "runtime/src/devflow_temporal"
    result = set()
    for parent, directories, files in os.walk(package, followlinks=False):
        directories[:] = [name for name in directories if name != "__pycache__"]
        for name in directories:
            if (Path(parent) / name).is_symlink():
                raise ValueError("qualified SDK package contains an alias")
        for name in files:
            if name.endswith((".py", ".pyc", ".so", ".pyd")):
                result.add(str((Path(parent) / name).relative_to(root)))
    return result


def site_checks(root, manifest):
    site = root / manifest["site_root"]
    expected = {name for name in manifest["site"] if name.endswith(".pth")}
    observed = {str(file.relative_to(root)) for file in site.glob("*.pth")}
    if observed != expected:
        raise ValueError("qualified interpreter site registration changed")
    for name in ("sitecustomize", "usercustomize"):
        if any(site.glob(name + ".*")) or (site / name).exists():
            raise ValueError("qualified interpreter has an unregistered site customization")
    for file in (*site.glob("_virtualenv*"), *site.glob("devflow_temporal*")):
        relative = str(file.relative_to(root))
        if relative in manifest["site"] or file.name.endswith(".dist-info"):
            continue
        raise ValueError("qualified interpreter contains an unregistered source/import shadow")


def source_stamps(root, manifest, *, hashing):
    if importable(root) != {name for name in manifest["source"] if
            name.startswith("runtime/src/devflow_temporal/") and name.endswith((".py", ".pyc", ".so", ".pyd"))}:
        raise ValueError("qualified SDK import inventory changed or contains a shadow")
    site_checks(root, manifest)
    expected = {**manifest["source"], **manifest["site"],
                "runtime/.venv/pyvenv.cfg": manifest["venv_config_sha256"]}
    stamps = {}
    for name, expected_sha in expected.items():
        file = root / name
        if hashing:
            role = ("tracked_source" if name in manifest["source"] else
                    "registered_site" if name in manifest["site"] else "venv_configuration")
            raw, observed = opened(file, role=role)
            if sha(raw) != expected_sha:
                raise ValueError("qualified source bytes differ from the requested Git/environment manifest: " + name)
        else:
            observed = stamp(file.lstat())
        stamps[name] = observed
    return stamps


def check_image(root):
    value, folder, manifest, manifest_sha = active(root)
    if (git(manifest["git_bin"], root, "rev-parse", "HEAD").decode().strip() != manifest["revision"]
            or git(manifest["git_bin"], root, "rev-parse", "HEAD^{tree}").decode().strip() != manifest["tree"]
            or git(manifest["git_bin"], root, "status", "--porcelain", "--untracked-files=normal", "--", *SCOPES).strip()):
        raise ValueError("qualified source HEAD/tree or clean state changed before imports")
    files = source_stamps(root, manifest, hashing=True)
    base_raw, base_stamp = opened(Path(manifest["base_python"]), owned=False, role="base_interpreter")
    git_raw, git_stamp = opened(Path(manifest["git_bin"]), owned=False, role="git_observer")
    if sha(base_raw) != manifest["base_python_sha256"] or sha(git_raw) != manifest["git_sha256"]:
        raise ValueError("qualified interpreter or Git observer changed")
    return value, folder, manifest, manifest_sha, files, base_stamp, git_stamp


def start(root, birth, owner_fd, python_path):
    value, folder, manifest, manifest_sha, files, base_stamp, git_stamp = check_image(root)
    if birth != value["birth"]:
        raise ValueError("qualified source birth changed before startup")
    if Path(python_path).resolve(strict=True) != Path(manifest["base_python"]):
        raise ValueError("qualified environment interpreter changed")
    actual, named = os.fstat(owner_fd), (value["owner"] / ".production-install.lock").stat()
    if (actual.st_dev, actual.st_ino) != (named.st_dev, named.st_ino):
        raise ValueError("qualified source did not retain its actual owner lease")
    record = {"schema": SCHEMA, "pid": os.getpid(), "root": str(root), "birth_sha256": value["birth_sha256"],
              "manifest_sha256": manifest_sha, "owner_fd": owner_fd, "owner_node": [actual.st_dev, actual.st_ino],
              "python": str(python_path), "files": files, "base_stamp": base_stamp, "git_stamp": git_stamp,
              "head_stamp": stamp((root / ".git/HEAD").lstat())}
    directory = value["owner"] / ".production-process-starts"
    info = directory.lstat()
    if directory.resolve() != directory or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError("qualified process witness directory is not private")
    writer, name = tempfile.mkstemp(prefix="source-start-", dir=directory)
    try:
        with os.fdopen(writer, "wb") as stream:
            stream.write(json.dumps(record, sort_keys=True).encode())
            stream.flush()
            os.fsync(stream.fileno())
        reader = os.open(name, os.O_RDONLY | os.O_NOFOLLOW)
        os.unlink(name)
        os.set_inheritable(reader, True)
        return reader
    finally:
        if os.path.lexists(name):
            os.unlink(name)


def process_record():
    text = os.environ.get(FD_ENV, "")
    if not text.isdigit() or int(text) < 3:
        raise ValueError("fresh namespace requires a managed start through its installed entry before imports")
    descriptor = int(text)
    info = os.fstat(descriptor)
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 0
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_size > LIMIT
            or fcntl.fcntl(descriptor, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY):
        raise ValueError("qualified source startup witness is not the retained read-only object")
    raw = os.pread(descriptor, info.st_size + 1, 0)
    if len(raw) != info.st_size:
        raise ValueError("qualified source process witness is incomplete")
    value = json.loads(raw)
    if value.get("schema") != SCHEMA or value.get("pid") != os.getpid():
        raise ValueError("qualified source witness belongs to another process; child startup must be fresh")
    return value


def verify(root):
    record = process_record()
    value, _folder, manifest, manifest_sha = active(root)
    if (record.get("root") != str(root) or record.get("birth_sha256") != value["birth_sha256"]
            or record.get("manifest_sha256") != manifest_sha
            or Path(sys.prefix) != root / "runtime/.venv" or str(sys.executable) != record["python"]
            or sys.pycache_prefix != CACHE or not sys.dont_write_bytecode):
        raise ValueError("qualified source process no longer matches its active startup")
    owner_fd = record["owner_fd"]
    actual, named = os.fstat(owner_fd), (value["owner"] / ".production-install.lock").stat()
    if [actual.st_dev, actual.st_ino] != record["owner_node"] or record["owner_node"] != [named.st_dev, named.st_ino]:
        raise ValueError("qualified source lost its actual lifetime owner")
    fcntl.flock(owner_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    if (source_stamps(root, manifest, hashing=False) != record["files"]
            or stamp((root / ".git/HEAD").lstat()) != record["head_stamp"]
            or stamp(Path(manifest["base_python"]).lstat()) != record["base_stamp"]
            or stamp(Path(manifest["git_bin"]).lstat()) != record["git_stamp"]):
        raise ValueError("qualified source/interpreter changed since pre-import startup; restart after reconciliation")
    for name, module in tuple(sys.modules.items()):
        if name != "devflow_temporal" and not name.startswith("devflow_temporal."):
            continue
        origin = getattr(module, "__file__", None)
        if not isinstance(origin, str) or not origin:
            raise ValueError("loaded SDK module has no measured source origin: " + name)
        file = Path(origin)
        relative = str(file.relative_to(root)) if file.is_relative_to(root) else None
        spec = getattr(module, "__spec__", None)
        if (relative not in manifest["source"] or file.resolve(strict=True) != file
                or spec is None or spec.origin != str(file)):
            raise ValueError("loaded SDK module is outside the measured generation: " + name)
    return {"source_root": str(root), "source_revision": manifest["revision"], "source_tree": manifest["tree"],
            "birth_sha256": value["birth_sha256"], "source_manifest_sha256": manifest_sha}
